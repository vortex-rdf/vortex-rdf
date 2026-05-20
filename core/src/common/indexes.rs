use crate::error::{Result, VortexRdfError};
use clap::ValueEnum;
use vortex_array::arrays::{ListArray, PrimitiveArray};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, ToCanonical};
use vortex_dtype::DType;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum IndexType {
    SimpleDictionary,
    ChainedHash,
}

impl IndexType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            IndexType::SimpleDictionary => "simple-dictionary",
            IndexType::ChainedHash => "chained-hash",
        }
    }
}

pub fn detect_index_type(array: &ArrayRef) -> IndexType {
    if let DType::Struct(fields, _) = array.dtype() {
        if fields.names().iter().any(|n| n.as_ref() == "store_type") {
            // Attempt to read the store_type from the first row
            let slice = if array.len() > 0 {
                array.slice(0..1)
            } else {
                array.clone()
            };

            let struct_arr = slice.to_struct();
            if let Some(idx) = struct_arr
                .names()
                .iter()
                .position(|n| n.as_ref() == "store_type")
            {
                if let Some(col) = struct_arr.fields().get(idx) {
                    let scalar = col.scalar_at(0);
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
    // Fallback to ChainedHash
    IndexType::ChainedHash
}

pub fn wrap_array_in_list(array: ArrayRef) -> Result<ArrayRef> {
    log::trace!("Wrapping array of length {} in a list array.", array.len());
    if array.len() > i32::MAX as usize {
        log::warn!("Array length {} exceeds i32::MAX, consider using i64 offsets for list wrapping", array.len());
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
