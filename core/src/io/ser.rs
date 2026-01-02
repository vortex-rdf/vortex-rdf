use crate::store::dictionary::Dictionary;
use crate::error::Result;
use oxrdf::Quad;
use vortex_array::arrays::{ListArray, PrimitiveArray, StructArray, VarBinViewArray};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_dtype::{DType, Nullability};
use vortex_fsst::{fsst_compress, fsst_train_compressor};
use vortex::compressor::CompactCompressor;
use vortex::file::WriteStrategyBuilder;
use vortex_session::VortexSession;
use vortex_file::WriteOptionsSessionExt;
use vortex_io::VortexWrite;
use vortex::VortexSessionDefault;

pub fn bundle_as_struct(
    dict: Dictionary,
    s_ids: Vec<u32>,
    p_ids: Vec<u32>,
    o_ids: Vec<u32>,
    g_ids: Vec<u32>,
) -> Result<ArrayRef> {
    // Compress the dictionary using FSST
    let dict_start = std::time::Instant::now();
    let dict_arr = encode_dictionary(&dict)?;
    log::debug!("[ser::bundle_as_struct] Dictionary FSST encoding took {:?}", dict_start.elapsed());
    // Build an initial StructArray with all the (SPOG) columns
    let quads_start = std::time::Instant::now();
    let quads_struct = encode_quad_ids(s_ids, p_ids, o_ids, g_ids)?;
    log::debug!("[ser::bundle_as_struct] Quad IDs struct encoding took {:?}", quads_start.elapsed());
    
    /*
      Bundle everything into an overarching root StructArray using the ListArray trick for differing lengths.
      We create a StructArray with two fields: "dictionary" and "quads" that looks like this:
      { 
         "dictionary": [
             dict_arr: VarBinViewArray (FSST compressed)
         ]: ListArray, 
         "quads": [
             quads_struct: {
                 s: PrimitiveArray,
                 p: PrimitiveArray,
                 o: PrimitiveArray,
                 g: PrimitiveArray
             }: StructArray
         ]: ListArray 
      }
      
      The ListArray trick is used to homogenize the array lengths, where both the "dictionary" and "quads"
      become ListArrays of length 1, where the offsets define the boundaries of the single element containing the entire original array.
     */
    let bundle_start = std::time::Instant::now();
    let dict_offsets = PrimitiveArray::from_iter(vec![0i32, dict_arr.len() as i32]).into_array();
    let dict_list = ListArray::try_new(dict_arr, dict_offsets, Validity::NonNullable)?.into_array();
    let quads_offsets =
        PrimitiveArray::from_iter(vec![0i32, quads_struct.len() as i32]).into_array();
    let quads_list =
        ListArray::try_new(quads_struct, quads_offsets, Validity::NonNullable)?.into_array();
    let root =
        StructArray::from_fields(&[("dictionary", dict_list), ("quads", quads_list)])?.into_array();
    log::debug!("[ser::bundle_as_struct] Root struct encoding took {:?}", bundle_start.elapsed());
    Ok(root)
}

pub fn encode_quads<I>(quads: I) -> Result<ArrayRef>
where
    I: IntoIterator<Item = Quad>,
{
    let mut dict = Dictionary::new();
    let mut s_ids = Vec::new();
    let mut p_ids = Vec::new();
    let mut o_ids = Vec::new();
    let mut g_ids = Vec::new();

    for quad in quads {
        s_ids.push(dict.get_or_insert(&quad.subject.to_string()));
        p_ids.push(dict.get_or_insert(&quad.predicate.to_string()));
        o_ids.push(dict.get_or_insert(&quad.object.to_string()));
        g_ids.push(dict.get_or_insert(&quad.graph_name.to_string()));
    }

    bundle_as_struct(dict, s_ids, p_ids, o_ids, g_ids)
}

pub fn encode_dictionary(dict: &Dictionary) -> Result<ArrayRef> {
    let dict_raw = VarBinViewArray::from_iter(
        dict.terms.iter().map(|s: &String| Some(s.as_str())),
        DType::Utf8(Nullability::NonNullable),
    );

    // Apply FSST compression to the dictionary table
    if dict_raw.len() > 0 {
        let compressor = fsst_train_compressor(&dict_raw);
        Ok(fsst_compress(dict_raw, &compressor).into_array())
    } else {
        Ok(dict_raw.into_array())
    }
}

pub fn encode_quad_ids(
    s_ids: Vec<u32>,
    p_ids: Vec<u32>,
    o_ids: Vec<u32>,
    g_ids: Vec<u32>,
) -> Result<ArrayRef> {
    let s_arr = PrimitiveArray::from_iter(s_ids).into_array();

    // Check if predicates fit in u16
    let p_arr = if p_ids.iter().all(|&id| id <= u16::MAX as u32) {
        PrimitiveArray::from_iter(p_ids.into_iter().map(|id| id as u16)).into_array()
    } else {
        PrimitiveArray::from_iter(p_ids).into_array()
    };

    let o_arr = PrimitiveArray::from_iter(o_ids).into_array();
    let g_arr = PrimitiveArray::from_iter(g_ids).into_array();

    Ok(
        StructArray::from_fields(&[("s", s_arr), ("p", p_arr), ("o", o_arr), ("g", g_arr)])?
            .into_array(),
    )
}

pub async fn write_array_to_vortex<W: VortexWrite + Unpin + Send>(
    array: ArrayRef,
    mut writer: W,
) -> Result<()> {
    let session_start = std::time::Instant::now();
    let session = VortexSession::default();

    let write_opts = session.write_options().with_strategy(
        WriteStrategyBuilder::new()
            .with_compressor(CompactCompressor::default())
            .build(),
    );
    log::debug!("[ser::write_array_to_vortex] Vortex writer options setup took {:?}", session_start.elapsed());

    let stream_start = std::time::Instant::now();
    let stream = array.to_array_stream();
    log::debug!("[ser::write_array_to_vortex] Array stream creation took {:?}", stream_start.elapsed());

    let write_start = std::time::Instant::now();
    let _summary = write_opts
        .write(&mut writer, stream)
        .await
        .map_err(|e: vortex_error::VortexError| crate::error::VortexRdfError::from(e))?;
    log::debug!("[ser::write_array_to_vortex] Vortex writing took {:?}", write_start.elapsed());

    Ok(())
}

pub fn write_array_to_ipc<W: std::io::Write>(array: ArrayRef, mut writer: W) -> Result<()> {
    use vortex_ipc::iterator::ArrayIteratorIPC;
    let ipc_iter = array.to_array_iterator().into_ipc();

    for msg_res in ipc_iter {
        let msg = msg_res.map_err(crate::error::VortexRdfError::Vortex)?;
        writer
            .write_all(&msg)
            .map_err(|e| crate::error::VortexRdfError::Serialization(e.to_string()))?;
    }

    Ok(())
}
