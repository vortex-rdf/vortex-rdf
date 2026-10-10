//! The column names that define the serialized format.
//!
//! These are contract, not policy: a name change here changes what a written
//! file means, so they live in one place, and a child that carries a primary
//! column under the primary's name (the copy index families) uses these
//! constants rather than its own spelling. Column names owned by a single
//! subsystem live with that subsystem instead: the row id every index child
//! carries in the index hub ([`indexes::COL_RID`]), the reference index's
//! `val` in [`secondary_by_reference`], the dictionary child's `_dict_term`
//! in [`term_dict`](crate::store::layouts::dictionary::term_dict), and the
//! TypedObject layout's split object columns in [`typed_object`].
//!
//! [`indexes::COL_RID`]: crate::store::indexes::COL_RID
//! [`secondary_by_reference`]: crate::store::indexes::secondary_by_reference
//! [`typed_object`]: crate::store::layouts::typed_object

use vortex_array::dtype::{DType, Nullability, PType};

/// A term code: a term's rank in a Dictionary-layout store's sorted term
/// dictionary — the value of every code column (the quad table's `s`, `p`,
/// `o`, `g` and the index children's code columns) and of every code a
/// binding hands across. 64 bits wide, so a dictionary may hold more terms
/// than a `u32` can count.
pub type TermCode = u64;

/// The primitive type of a code column on the wire: non-nullable `u64`
/// ([`TermCode`]). A Dictionary-layout quad table is recognized by its `s`
/// column having this type; a code column of any other integer width is a
/// file this crate never wrote and is refused at open.
pub(crate) const CODE_PTYPE: PType = PType::U64;

/// A row id: a quad's position in the quad table — the value of every index
/// child's `rid` column ([`COL_RID`](crate::store::indexes::COL_RID)), which
/// is how an index names the rows it matched, and the currency a match's
/// row selection, tombstones and further matches share without renumbering
/// anything. 64 bits wide, so a store with secondary indexes may hold more
/// quads than a `u32` can count.
pub type RowId = u64;

/// The primitive type of a row-id column on the wire: non-nullable `u64`
/// ([`RowId`]). An index child whose `rid` column is an integer of any other
/// width is a file this crate never wrote and is refused at open.
pub(crate) const ROW_ID_PTYPE: PType = PType::U64;

/// Whether a column named `name` holds term codes where it is an integer:
/// the quad table's and the copy index children's `s`, `p`, `o`, `g`, and
/// the reference index children's `val`. Under the string layouts the same
/// names hold term strings.
pub(crate) fn is_code_column_name(name: &str) -> bool {
    PRIMARY_COLUMNS.contains(&name)
        || name == crate::store::indexes::secondary_by_reference::COL_VAL
}

/// Whether the field `name: dtype` is a term-code column as this crate
/// writes one: a code column name ([`is_code_column_name`]) holding
/// non-nullable [`CODE_PTYPE`] values.
pub(crate) fn is_code_field(name: &str, dtype: &DType) -> bool {
    is_code_column_name(name)
        && matches!(dtype, DType::Primitive(ptype, Nullability::NonNullable) if *ptype == CODE_PTYPE)
}

/// The subject column — first in every layout. Whether its rows are globally
/// sorted is per-store provenance
/// ([`quads_sorted`](crate::io::container::layout::quads_sorted)), not a
/// property of the name.
pub(crate) const COL_S: &str = "s";
/// The predicate column.
pub(crate) const COL_P: &str = "p";
/// The object column (`o_value` under the TypedObject layout's split form).
pub(crate) const COL_O: &str = "o";
/// The graph-name column (empty string = default graph).
pub(crate) const COL_G: &str = "g";

/// The four primary columns in emission order.
pub(crate) const PRIMARY_COLUMNS: [&str; 4] = [COL_S, COL_P, COL_O, COL_G];

/// One of the four primary quad columns, by role — how the narrowing
/// surface ([`keep`](crate::store::VortexRdfStore::keep)) and the code
/// payloads name a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum QuadColumn {
    /// The subject.
    S,
    /// The predicate.
    P,
    /// The object.
    O,
    /// The graph name.
    G,
}

impl QuadColumn {
    /// The four columns in emission order — the order of
    /// [`PRIMARY_COLUMNS`] and of every `(s, p, o, g)` code payload.
    pub const ALL: [QuadColumn; 4] = [QuadColumn::S, QuadColumn::P, QuadColumn::O, QuadColumn::G];

    /// The column's name in the serialized schema.
    pub fn name(self) -> &'static str {
        PRIMARY_COLUMNS[self.index()]
    }

    /// The column's position in `(s, p, o, g)` order.
    pub fn index(self) -> usize {
        match self {
            QuadColumn::S => 0,
            QuadColumn::P => 1,
            QuadColumn::O => 2,
            QuadColumn::G => 3,
        }
    }

    /// The column at `index` in `(s, p, o, g)` order, `None` past the fourth.
    pub fn from_index(index: usize) -> Option<Self> {
        Self::ALL.get(index).copied()
    }

    /// The column named `name` (`s`, `p`, `o` or `g`), `None` for any other.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.name() == name)
    }
}
