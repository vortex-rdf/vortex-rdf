//! The persisted primary column names: the four quad columns and the
//! TypedObject layout's object sub-columns. A name change here changes what
//! a written file means. Index children add `rid` (`indexes::COL_RID`); the
//! dictionary child is `_dict_term` (`dictionary::storage::COL_DICT_TERM`).

/// The subject column, first in every layout.
pub(crate) const COL_S: &str = "s";
/// The predicate column.
pub(crate) const COL_P: &str = "p";
/// The object column (`o_value` under the TypedObject layout).
pub(crate) const COL_O: &str = "o";
/// The graph-name column; the empty string is the default graph.
pub(crate) const COL_G: &str = "g";

/// The four primary columns in emission order.
pub(crate) const PRIMARY_COLUMNS: [&str; 4] = [COL_S, COL_P, COL_O, COL_G];

/// TypedObject: the object's kind tag (0 IRI, 1 blank node, 2 plain literal,
/// 3 language-tagged literal, 4 typed literal); its presence marks the
/// layout.
pub(crate) const COL_O_KIND: &str = "o_kind";
/// TypedObject: the object's lexical value (IRI, blank node id or literal
/// value).
pub(crate) const COL_O_VALUE: &str = "o_value";
/// TypedObject: the literal datatype IRI, null unless the object is a typed
/// literal.
pub(crate) const COL_O_DATATYPE: &str = "o_datatype";
/// TypedObject: the literal language tag, null unless the object is a
/// language-tagged literal.
pub(crate) const COL_O_LANG: &str = "o_lang";

/// One of the four primary quad columns, by role; the discriminant is the
/// column's position in `(s, p, o, g)` order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum QuadColumn {
    /// The subject.
    S = 0,
    /// The predicate.
    P = 1,
    /// The object.
    O = 2,
    /// The graph name.
    G = 3,
}

impl QuadColumn {
    /// The four columns in emission order.
    pub const ALL: [QuadColumn; 4] = [QuadColumn::S, QuadColumn::P, QuadColumn::O, QuadColumn::G];

    /// The column's name in the serialized schema.
    pub fn name(self) -> &'static str {
        PRIMARY_COLUMNS[self.index()]
    }

    /// The column's position in `(s, p, o, g)` order.
    pub fn index(self) -> usize {
        self as usize
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
