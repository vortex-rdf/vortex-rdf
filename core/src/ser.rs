use crate::error::Result;
use crate::Dictionary;
use oxrdf::Quad;
use vortex_array::arrays::{ListArray, PrimitiveArray, StructArray, VarBinViewArray};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_dtype::{DType, Nullability};

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
        s_ids.push(dict.get_or_insert_subject(&quad.subject));
        p_ids.push(dict.get_or_insert_named_node(&quad.predicate));
        o_ids.push(dict.get_or_insert_term(&quad.object));
        g_ids.push(dict.get_or_insert_graph(&quad.graph_name));
    }

    let quads_struct = encode_quad_ids(s_ids, p_ids, o_ids, g_ids)?;

    let dict_arr = encode_dictionary(&dict)?;

    // Bundle into a root StructArray using the ListArray trick for differing lengths.
    let dict_offsets = PrimitiveArray::from_iter(vec![0i32, dict_arr.len() as i32]).into_array();
    let dict_list = ListArray::try_new(dict_arr, dict_offsets, Validity::NonNullable)?.into_array();

    let quads_offsets =
        PrimitiveArray::from_iter(vec![0i32, quads_struct.len() as i32]).into_array();
    let quads_list =
        ListArray::try_new(quads_struct, quads_offsets, Validity::NonNullable)?.into_array();

    let root =
        StructArray::from_fields(&[("dictionary", dict_list), ("quads", quads_list)])?.into_array();

    Ok(root)
}

pub fn encode_dictionary(dict: &Dictionary) -> Result<ArrayRef> {
    let dict_raw = VarBinViewArray::from_iter(
        dict.terms.iter().map(|s| Some(s.as_str())),
        DType::Utf8(Nullability::NonNullable),
    );

    // Apply FSST compression to the dictionary table
    if dict_raw.len() > 0 {
        use vortex_fsst::{fsst_compress, fsst_train_compressor};
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

    // Check if predicates fit in u16 (very common optimization in RDF stores)
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
