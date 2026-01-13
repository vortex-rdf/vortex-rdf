use clap::ValueEnum;
use vortex_array::{ToCanonical, ArrayRef};
use vortex_dtype::DType;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum IndexType {
    Dictionary,
    ChainedHash,
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
            if let Some(idx) = struct_arr.names().iter().position(|n| n.as_ref() == "store_type") {
                 if let Some(col) = struct_arr.fields().get(idx) {
                     let scalar = col.scalar_at(0);
                     let val = format!("{}", scalar); 
                     if val.contains("chained-hash") { return IndexType::ChainedHash; }
                     if val.contains("dictionary") { return IndexType::Dictionary; }
                 }
            }
        }
    }
    // Fallback to ChainedHash
    IndexType::ChainedHash
}