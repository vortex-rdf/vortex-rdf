//! The column names that define the serialized format.
//!
//! These are contract, not policy: a name change here changes what a written
//! file means, so they live in one place. Column names owned by a single
//! subsystem live with that subsystem instead: index column names (`_idx_*`)
//! in their index modules ([`secondary_by_copy::Family`],
//! [`secondary_by_reference`]), the dictionary child's `_dict_term` in
//! [`term_dict`](crate::store::layouts::dictionary::term_dict), and the
//! TypedObject layout's split object columns in [`typed_object`].
//!
//! [`secondary_by_copy::Family`]: crate::store::indexes::secondary_by_copy::Family
//! [`secondary_by_reference`]: crate::store::indexes::secondary_by_reference
//! [`typed_object`]: crate::store::layouts::typed_object

/// The subject column — first in every layout, globally sorted by the sorted
/// builders (which stamp `IsSorted` on it for the binary-search fast path).
pub(crate) const COL_S: &str = "s";
/// The predicate column.
pub(crate) const COL_P: &str = "p";
/// The object column (`o_value` under the TypedObject layout's split form).
pub(crate) const COL_O: &str = "o";
/// The graph-name column (empty string = default graph).
pub(crate) const COL_G: &str = "g";

/// The four primary columns in emission order.
pub(crate) const PRIMARY_COLUMNS: [&str; 4] = [COL_S, COL_P, COL_O, COL_G];
