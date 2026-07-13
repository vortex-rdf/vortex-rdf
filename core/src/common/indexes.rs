use crate::error::{Result, VortexRdfError};
use clap::ValueEnum;
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_array::arrays::dict::DictArraySlotsExt;
use vortex_array::arrays::listview::ListViewArrayExt;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{
    ChunkedArray, DictArray, ListArray, ListViewArray, PrimitiveArray, StructArray,
};
use vortex_array::dtype::DType;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, legacy_session, VortexSessionExecute};

/// The supported Vortex-RDF dictionary/indexing strategies.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum IndexType {
    /// A flat, simple dictionary mapper.
    SimpleDictionary,
    /// A double-hashed, chained indexing strategy.
    ChainedHash,
}

impl IndexType {
    /// String representation matching command line arguments.
    pub const fn as_str(&self) -> &'static str {
        match self {
            IndexType::SimpleDictionary => "simple-dictionary",
            IndexType::ChainedHash => "chained-hash",
        }
    }
}

/// Detects the dictionary index type directly from a serialized Vortex array.
/// Looks for a `"store_type"` column and parses its scalar value.
pub fn detect_index_type(array: &ArrayRef) -> IndexType {
    if let DType::Struct(fields, _) = array.dtype() {
        // Look for the "store_type" column in the struct schema.
        if fields.names().iter().any(|n| n.as_ref() == "store_type") {
            // Read only the first row/slice for zero-cost index type detection.
            let slice = if array.len() > 0 {
                array.slice(0..1).unwrap_or_else(|_| array.clone())
            } else {
                array.clone()
            };
            let session = VortexSession::default(); // default session (registries etc.)
            let mut ctx = session.create_execution_ctx(); // execution ctx
            if let Ok(struct_arr) = slice.clone().execute::<StructArray>(&mut ctx) {
                if let Some(idx) = struct_arr
                    .names()
                    .iter()
                    .position(|n| n.as_ref() == "store_type")
                {
                    if let Some(col) = struct_arr.unmasked_fields().get(idx) {
                        // Execute and read the scalar value at row 0.
                        if let Ok(scalar) = col.execute_scalar(0, &mut ctx) {
                            let val = format!("{}", scalar);
                            if val.contains("chained-hash") {
                                return IndexType::ChainedHash;
                            }
                            if val.contains("simple-dictionary") {
                                return IndexType::SimpleDictionary;
                            }
                        }
                    }
                }
            }
        }
    }
    // Default fallback index type is ChainedHash.
    IndexType::ChainedHash
}

/// Helper function to wrap a flat Vortex array into a ListArray containing exactly one list element.
pub fn wrap_array_in_list(array: ArrayRef) -> Result<ArrayRef> {
    log::trace!("Wrapping array of length {} in a list array.", array.len());
    if array.len() > i32::MAX as usize {
        log::warn!(
            "Array length {} exceeds i32::MAX, consider using i64 offsets for list wrapping",
            array.len()
        );
        return Err(VortexRdfError::Deserialization(format!(
            "Array length {} exceeds i32::MAX, cannot wrap in list",
            array.len()
        )));
    }
    let offsets = PrimitiveArray::from_iter(vec![0i32, array.len() as i32]).into_array();
    let list = ListArray::try_new(array, offsets, Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)?
        .into_array();
    Ok(list)
}

/// Wraps any arbitrary array (e.g. hash tables or simple term dictionaries)
/// inside a single-slot `DictArray`. This is the core architectural layout pattern
/// we use to keep nested index structures flat and zero-copy.
///
/// The target array is stored exactly once in `values` (as a `ListArray` of length 1).
/// The `codes` array is a `PrimitiveArray` of length `n` filled with all zeros,
/// which compresses down to a few bytes.
pub fn array_as_dict_column(array: ArrayRef, n: usize) -> Result<ArrayRef> {
    let values = wrap_array_in_list(array)?;
    let codes = PrimitiveArray::from_iter(vec![0u32; n]).into_array();
    DictArray::try_new(codes, values)
        .map(|a| a.into_array())
        .map_err(VortexRdfError::Vortex)
}

/// Extracts the underlying array stored using the zero-copy `array_as_dict_column` wrapper.
/// Supports both flat `DictArray` columns and multi-chunked columns (`ChunkedArray`) safely.
pub fn array_from_dict_column(array: &ArrayRef) -> Result<ArrayRef> {
    let mut ctx = legacy_session().create_execution_ctx();

    // 1. Resolve chunked columns by targeting the first chunk.
    let target_array = if array.encoding_id().as_ref() == "vortex.chunked" {
        use vortex_array::arrays::chunked::ChunkedArrayExt;
        let chunked_arr = ChunkedArray::try_from_array_ref(array.clone()).map_err(|_| {
            VortexRdfError::Deserialization("Failed to cast array to ChunkedArray".to_string())
        })?;
        if chunked_arr.nchunks() == 0 {
            return Err(VortexRdfError::Deserialization(
                "Chunked dictionary column has 0 chunks".to_string(),
            ));
        }
        chunked_arr.chunk(0).clone()
    } else {
        array.clone()
    };

    // 2. Cast the array reference to a typed DictArray.
    let dict_arr = DictArray::try_from_array_ref(target_array).map_err(|e| {
        VortexRdfError::Deserialization(format!(
            "Array is not a DictArray: encoding {:?}, error: {}",
            array.encoding_id(),
            e
        ))
    })?;

    // 3. Extract the underlying list array elements from the single slot of values.
    let values_list = dict_arr.values().clone();
    let list_arr = values_list
        .execute::<ListViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(list_arr.elements().clone())
}
