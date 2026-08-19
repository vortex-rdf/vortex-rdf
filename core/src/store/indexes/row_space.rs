//! The `_idx_` row-space column convention: which columns of a welded row
//! space belong to a secondary index rather than the primary quads.
//!
//! The prefix is wire-visible (it is how a schema marks index columns), so it
//! must have exactly one owner — [`is_index_column`] — and both strips derive
//! from it: the array-level projection ([`strip_index_columns`]) and the
//! dtype-level one ([`primary_dtype`], the tee's schema counterpart). A
//! future index whose columns broke the convention (or a prefix change) is
//! then caught here once, instead of the tee'd file and the in-memory split
//! disagreeing about what is primary.

use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::{DType, FieldName};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;

/// Whether a row-space column name marks a secondary-index column — the
/// single encoding of the `_idx_` prefix convention.
pub(crate) fn is_index_column(name: &str) -> bool {
    name.starts_with("_idx_")
}

/// Project away secondary-index columns (`_idx_*`), keeping the layout's
/// primary columns. Returns the array unchanged when it carries no index
/// columns.
pub(crate) fn strip_index_columns(arr: ArrayRef) -> Result<ArrayRef> {
    // Figure out which field names to keep; bail out unchanged if there are
    // no `_idx_*` columns to strip in the first place (the common case).
    let keep: Vec<FieldName> = match arr.dtype() {
        DType::Struct(fields, _) if fields.names().iter().any(|n| is_index_column(n.as_ref())) => {
            fields
                .names()
                .iter()
                .filter(|n| !is_index_column(n.as_ref()))
                .cloned()
                .collect()
        }
        _ => return Ok(arr),
    };

    // Rebuild the struct with only the kept columns.
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = arr
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let arrays: Vec<ArrayRef> = keep
        .iter()
        .map(|n| {
            struct_arr
                .unmasked_field_by_name(n.as_ref())
                .cloned()
                .map_err(VortexRdfError::Vortex)
        })
        .collect::<Result<_>>()?;
    let len = struct_arr.len();
    StructArray::try_new(keep.into(), arrays, len, Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
}

/// The primary (non-`_idx_*`) projection of a row-space dtype.
// Consumed only by the tee (the streaming write path's schema split), which
// compiles with `file-io`.
#[cfg(feature = "file-io")]
pub(crate) fn primary_dtype(dtype: &DType) -> DType {
    use vortex_array::dtype::{Nullability, StructFields};
    match dtype {
        DType::Struct(fields, _) if fields.names().iter().any(|n| is_index_column(n.as_ref())) => {
            let keep: Vec<usize> = (0..fields.names().len())
                .filter(|&i| !is_index_column(fields.names()[i].as_ref()))
                .collect();
            DType::Struct(
                StructFields::new(
                    keep.iter()
                        .map(|&i| fields.names()[i].clone())
                        .collect::<Vec<_>>()
                        .into(),
                    keep.iter()
                        .map(|&i| fields.fields().nth(i).expect("index in range"))
                        .collect(),
                ),
                Nullability::NonNullable,
            )
        }
        other => other.clone(),
    }
}
