use crate::error::{Result, VortexRdfError};
use crate::store::dictionary::Dictionary;
use oxrdf::{Quad, Subject, Term};
use vortex_array::ArrayRef;
use vortex_array::ToCanonical;
use vortex_ipc::iterator::SyncIPCReader;
use futures::{Stream, TryStreamExt};
use vortex_array::session::ArraySession;
use vortex_session::VortexSession;
use vortex_file::OpenOptionsSessionExt;
use vortex_scalar::Scalar;
use vortex_io::file::IntoReadSource;
use vortex_array::stream::ArrayStreamExt;
use vortex::VortexSessionDefault;
use vortex_dtype::{DType, Nullability, PType};

pub fn array_from_reader<R: std::io::Read>(reader: R) -> Result<ArrayRef> {
    let array_session = ArraySession::default();
    let registry = array_session.registry();

    // Register FSST encoding - check where it moved
    // registry.register(EncodingRef::new_ref(vortex_fsst::FSSTEncoding.as_ref()));

    let mut reader =
        SyncIPCReader::try_new(reader, registry.clone()).map_err(crate::VortexRdfError::Vortex)?;

    let array = reader
        .next()
        .transpose()
        .map_err(VortexRdfError::Vortex)?
        .ok_or_else(|| VortexRdfError::Deserialization("No array in IPC stream".to_string()))?;

    Ok(array)
}

pub async fn read_array_from_vortex<S: IntoReadSource>(
    source: S,
) -> Result<ArrayRef> {
    let start = std::time::Instant::now();
    let session = VortexSession::default();

    let file = session.open_options()
        .open(source)
        .await
        .map_err(|e| crate::VortexRdfError::from(e))?;
    log::debug!("[de::read_array_from_vortex] File open Session took {:?}", start.elapsed());

    let scan_start = std::time::Instant::now();
    let scan = file.scan().map_err(|e| crate::VortexRdfError::from(e))?;    
    let stream = scan.into_array_stream().map_err(|e| crate::VortexRdfError::from(e))?;
    log::debug!("[de::read_array_from_vortex] Scan took {:?}", scan_start.elapsed());
    
    let read_start = std::time::Instant::now();
    let array: ArrayRef = stream
        .read_all()
        .await
        .map_err(|e: vortex_error::VortexError| crate::VortexRdfError::from(e))?;
    log::debug!("[de::read_array_from_vortex] Stream read_all took {:?}", read_start.elapsed());

    Ok(array)
}

pub async fn decode_quads(root: ArrayRef) -> Result<Vec<Quad>> {
    decode_quads_stream(root)?.try_collect().await
}

pub fn decode_quads_stream(root: ArrayRef) -> Result<impl Stream<Item = Result<Quad>>> {
    let start = std::time::Instant::now();
    let dictionary = Dictionary::from_root(&root)?;
    log::debug!("[de::decode_quads_stream] Dictionary rebuild took {:?}", start.elapsed());

    let quads_start = std::time::Instant::now();
    let root_struct = root.to_struct();

    let quads_list_ref = root_struct
        .fields()
        .get(1)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing quads field".to_string()))?
        .clone();

    let quads_list = quads_list_ref.to_listview();
    let offsets_scalar = quads_list.offsets().scalar_at(0);
    let sizes_scalar = quads_list.sizes().scalar_at(0);
    
    let quads_offset: usize = offsets_scalar
        .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)
        .and_then(
            |scalar: Scalar| scalar
                .as_primitive()
                .typed_value::<i32>()
                .ok_or_else(|| VortexRdfError::Deserialization("Missing quads offset".to_string()))
        )
        .map(|offset| offset as usize)?;
    let quads_size: usize = sizes_scalar
        .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)
        .and_then(
            |scalar: Scalar| scalar
                .as_primitive()
                .typed_value::<i32>()
                .ok_or_else(|| VortexRdfError::Deserialization("Missing quads size".to_string()))
        )
        .map(|size| size as usize)?;
    let quads_array_ref = quads_list
        .elements()
        .slice(quads_offset..quads_offset + quads_size);

    let quads_struct = quads_array_ref.to_struct();
    let fields = quads_struct.fields();

    let s_ids = fields.get(0)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing S IDs".to_string()))?
        .clone().to_primitive();
    let p_ids = fields.get(1)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing P IDs".to_string()))?
        .clone().to_primitive();
    let o_ids = fields.get(2)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing O IDs".to_string()))?
        .clone().to_primitive();
    let g_ids = fields.get(3)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing G IDs".to_string()))?
        .clone().to_primitive();
    
    log::debug!("[de::decode_quads_stream] Quads struct extraction took {:?}", quads_start.elapsed());

    let len = s_ids.len();

    let iter = (0..len).map(move |i| {
        let s_id = s_ids.as_slice::<u32>()[i];
        let p_id = match p_ids.ptype() {
            vortex_dtype::PType::U16 => p_ids.as_slice::<u16>()[i] as u32,
            vortex_dtype::PType::U32 => p_ids.as_slice::<u32>()[i],
            _ => {
                // This shouldn't happen if we validated above, but let's handle it
                // To avoid returning Result in every field access.
                0 // Fallback
            }
        };
        let o_id = o_ids.as_slice::<u32>()[i];
        let g_id = g_ids.as_slice::<u32>()[i];

        let s_term = dictionary
            .get_term(s_id)
            .ok_or_else(|| VortexRdfError::Deserialization(format!("S ID {} not in dictionary", s_id)))?;
        let p_term = dictionary
            .get_term(p_id)
            .ok_or_else(|| VortexRdfError::Deserialization(format!("P ID {} not in dictionary", p_id)))?;
        let o_term = dictionary
            .get_term(o_id)
            .ok_or_else(|| VortexRdfError::Deserialization(format!("O ID {} not in dictionary", o_id)))?;
        let g_name = dictionary
            .get_graph_name(g_id)
            .ok_or_else(|| VortexRdfError::Deserialization(format!("G ID {} not in dictionary", g_id)))?;

        let subject = match s_term {
            Term::NamedNode(n) => Subject::NamedNode(n),
            Term::BlankNode(b) => Subject::BlankNode(b),
            _ => return Err(VortexRdfError::Deserialization("Invalid subject type".to_string())),
        };

        let predicate = match p_term {
            Term::NamedNode(n) => n,
            _ => return Err(VortexRdfError::Deserialization("Invalid predicate type".to_string())),
        };

        Ok(Quad::new(subject, predicate, o_term, g_name))
    });

    Ok(futures::stream::iter(iter))
}
