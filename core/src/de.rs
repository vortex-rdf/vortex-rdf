use crate::error::{Result, VortexRdfError};
use crate::Dictionary;
use oxrdf::{Quad, Subject, Term};
use vortex_array::{ArrayRef, ToCanonical};

pub fn decode_quads(root: ArrayRef) -> Result<Vec<Quad>> {
    decode_quads_stream(root)?.collect()
}

pub fn decode_quads_stream(root: ArrayRef) -> Result<impl Iterator<Item = Result<Quad>>> {
    let dictionary = Dictionary::from_root(&root)?;
    let root_struct = root.to_struct();

    let quads_list_ref = root_struct
        .fields()
        .get(1)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing quads field".to_string()))?
        .clone();

    let quads_list = quads_list_ref.to_listview();
    let quads_offset = quads_list.offsets().to_primitive().as_slice::<i32>()[0] as usize;
    let quads_size = quads_list.sizes().to_primitive().as_slice::<i32>()[0] as usize;
    let quads_array_ref = quads_list
        .elements()
        .slice(quads_offset..quads_offset + quads_size);

    // Extract quads metadata
    let quads_struct = quads_array_ref.to_struct();
    let fields = quads_struct.fields();

    let s_ids_ref = fields
        .get(0)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing S IDs".to_string()))?
        .clone();
    let p_ids_ref = fields
        .get(1)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing P IDs".to_string()))?
        .clone();
    let o_ids_ref = fields
        .get(2)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing O IDs".to_string()))?
        .clone();
    let g_ids_ref = fields
        .get(3)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing G IDs".to_string()))?
        .clone();

    let s_ids = s_ids_ref.to_primitive();
    let p_ids = p_ids_ref.to_primitive();
    let o_ids = o_ids_ref.to_primitive();
    let g_ids = g_ids_ref.to_primitive();

    let len = s_ids.len();

    if !matches!(
        p_ids.ptype(),
        vortex_dtype::PType::U16 | vortex_dtype::PType::U32
    ) {
        return Err(VortexRdfError::Deserialization(format!(
            "Unsupported P ID type: {:?}",
            p_ids.ptype()
        )));
    }

    Ok((0..len).map(move |i| {
        let s_id = s_ids.as_slice::<u32>()[i];

        let p_id = match p_ids.ptype() {
            vortex_dtype::PType::U16 => p_ids.as_slice::<u16>()[i] as u32,
            vortex_dtype::PType::U32 => p_ids.as_slice::<u32>()[i],
            _pt => {
                // This shouldn't happen if we validated above, but let's handle it
                // To avoid returning Result in every field access, we could have pre-validated
                0 // Fallback
            }
        };

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
            Term::NamedNode(n) => Subject::NamedNode(n),
            Term::BlankNode(b) => Subject::BlankNode(b),
            _ => {
                return Err(VortexRdfError::Deserialization(
                    "Invalid subject type".to_string(),
                ))
            }
        };

        let predicate = match p_term {
            Term::NamedNode(n) => n,
            _ => {
                return Err(VortexRdfError::Deserialization(
                    "Invalid predicate type".to_string(),
                ))
            }
        };

        Ok(Quad::new(subject, predicate, o_term, g_name))
    }))
}

pub fn array_from_reader<R: std::io::Read>(reader: R) -> Result<ArrayRef> {
    use vortex_array::EncodingRef;
    use vortex_ipc::iterator::SyncIPCReader;

    let array_session = vortex_array::ArraySession::default();
    let registry = array_session.registry();

    // Manually register FSST and Dict encodings
    registry.register(EncodingRef::new_ref(vortex_fsst::FSSTEncoding.as_ref()));
    registry.register(EncodingRef::new_ref(
        vortex_array::arrays::DictEncoding.as_ref(),
    ));

    let mut reader =
        SyncIPCReader::try_new(reader, registry.clone()).map_err(VortexRdfError::Vortex)?;

    let array = reader
        .next()
        .transpose()
        .map_err(VortexRdfError::Vortex)?
        .ok_or_else(|| VortexRdfError::Deserialization("No array in IPC stream".to_string()))?;

    Ok(array)
}
