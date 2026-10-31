//! The expressions file reads push down over the quad table: the bind-memo
//! scope of its schema, the filter a pattern compiles to ([`PushedFilter`]),
//! and the code equalities read back out of such a filter once
//! ([`CodeEqs`]), which the reads by id check and the reads of a filter's
//! kept rows fill.

use std::borrow::Cow;
use std::sync::Arc;

use vortex_array::dtype::FieldName;
use vortex_array::expr::forms::conjuncts;
use vortex_array::expr::{Expression, and_collect, eq, get_item, lit, root};
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, IntoArray as _};
use vortex_buffer::Buffer;

use crate::error::{Result, VortexRdfError};
use crate::store::array::named_struct;
use crate::store::layouts::{Constraints, PatternCodes, QuadPattern};
use crate::store::schema::TermCode;

/// The bind-memo scope tag for expressions over the quad table's schema
/// (the transparent root the file scan reads).
pub(crate) const QUAD_SCOPE: &str = "quads";

/// The pushed-down filter for a pattern under `codes`' layout: `AND` of
/// `column == code` per compiled constraint, `lit(false)` for an unmatchable
/// pattern, `None` when nothing is bound.
pub(crate) fn build_file_filter(
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<Option<Expression>> {
    Ok(match codes.constraints(pattern)? {
        Constraints::AlwaysFalse => Some(lit(false)),
        Constraints::Eq(eqs) => eq_conjunction(eqs),
    })
}

/// The conjunction of `column == value` equalities over root fields, the
/// filter shape every pushed-down index probe and serve scan uses. `None`
/// for an empty constraint set.
pub(crate) fn eq_conjunction(
    constraints: impl IntoIterator<Item = (&'static str, Scalar)>,
) -> Option<Expression> {
    and_collect(
        constraints
            .into_iter()
            .map(|(column, value)| eq(get_item(column, root()), lit(value))),
    )
}

/// A filter a match pushed down to a file view: the expression the scans
/// evaluate, and the code equalities it is when it is a conjunction of them
/// ([`CodeEqs::of`]), read out of it once, when the view is built.
#[derive(Clone)]
pub(crate) struct PushedFilter {
    expr: Expression,
    eqs: Option<CodeEqs>,
}

impl PushedFilter {
    /// `expr` pushed down, its code equalities read out of it.
    pub(crate) fn new(expr: Expression) -> Self {
        let eqs = CodeEqs::of(&expr);
        Self { expr, eqs }
    }

    /// The expression the scans evaluate.
    pub(crate) fn expr(&self) -> &Expression {
        &self.expr
    }

    /// The code equalities the filter is, `None` for a filter of any other
    /// shape (the `AlwaysFalse` literal, a string layout's term equalities),
    /// which the reads by id cannot check per row.
    pub(crate) fn eqs(&self) -> Option<&CodeEqs> {
        self.eqs.as_ref()
    }
}

/// Point reads of fewer rows than this read every column, pinned or not:
/// filling the pinned ones costs a fixed few thousand instructions, more than
/// the values so few rows skip. On the bench store a pinned point read of
/// two rows cost 1.6 % more than reading all four columns (4 % for a quad or
/// chunk read), and one of 114 rows over two pinned columns 15 % less.
pub(crate) const PIN_POINT_MIN_ROWS: usize = 64;

/// The `(column, code)` pairs of a filter of code equalities, in the
/// filter's order. Every row such a filter keeps holds each code in its
/// column: the reads by id check them per row, and a read of rows that
/// passed the filter (its kept masks' rows, the ids a window resolved it
/// into) reads only the other columns and fills these with their codes
/// ([`fill_rows`](Self::fill_rows), [`fill_codes`](Self::fill_codes)).
#[derive(Clone, Debug, Default)]
pub(crate) struct CodeEqs(Option<Arc<[(FieldName, TermCode)]>>);

impl CodeEqs {
    /// No equality: a read of every column, filling none.
    pub(crate) const NONE: &'static Self = &Self(None);

    /// The equalities of a filter [`build_file_filter`] built, a conjunction
    /// of `column == literal` over root fields, each column the field name
    /// the filter holds (a reference-count bump, no copy). `None` for any
    /// other shape (the `AlwaysFalse` literal, say, or a string layout's
    /// term equalities): such a filter cannot be applied as per-row equality
    /// tests.
    pub(crate) fn of(filter: &Expression) -> Option<Self> {
        use vortex_array::scalar_fn::fns::binary::Binary;
        use vortex_array::scalar_fn::fns::get_item::GetItem;
        use vortex_array::scalar_fn::fns::literal::Literal;
        use vortex_array::scalar_fn::fns::operators::Operator;

        let pairs = conjuncts(filter)
            .iter()
            .map(|c| {
                let op = c.as_opt::<Binary>()?;
                if *op != Operator::Eq {
                    return None;
                }
                let field = c.child(0).as_opt::<GetItem>()?;
                let scalar = c.child(1).as_opt::<Literal>()?;
                let code = crate::store::array::unsigned_value(scalar)?;
                Some((field.clone(), code))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self((!pairs.is_empty()).then(|| Arc::from(pairs))))
    }

    /// Whether there is no equality.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    /// Each equality, `(column, code)`, in the filter's order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&FieldName, TermCode)> {
        self.0
            .iter()
            .flat_map(|pairs| pairs.iter().map(|(c, v)| (c, *v)))
    }

    /// How many equalities there are.
    pub(crate) fn len(&self) -> usize {
        self.0.as_deref().map_or(0, <[_]>::len)
    }

    /// The code `column` is pinned to.
    pub(crate) fn code(&self, column: &str) -> Option<TermCode> {
        self.iter()
            .find_map(|(name, code)| (name.as_ref() == column).then_some(code))
    }

    /// These equalities as a read of `rows` rows point by point fills them:
    /// all of them from [`PIN_POINT_MIN_ROWS`] rows, none below, where
    /// reading the pinned columns costs less than filling them. A take or a
    /// projection fills them all, whatever its rows.
    pub(crate) fn for_points(&self, rows: usize) -> &Self {
        if rows >= PIN_POINT_MIN_ROWS {
            self
        } else {
            Self::NONE
        }
    }

    /// The columns of `columns` a read of rows that passed these equalities
    /// reads, in order: the unpinned ones, or the first of `columns` when
    /// every one is pinned, so that the read still yields its rows;
    /// `columns` itself when none is.
    pub(crate) fn columns_to_read<'c>(
        &self,
        columns: &'c [&'static str],
    ) -> Cow<'c, [&'static str]> {
        if self.is_empty() {
            return Cow::Borrowed(columns);
        }
        let unpinned: Vec<&'static str> = columns
            .iter()
            .copied()
            .filter(|&c| self.code(c).is_none())
            .collect();
        if unpinned.is_empty() {
            Cow::Borrowed(&columns[..columns.len().min(1)])
        } else {
            Cow::Owned(unpinned)
        }
    }

    /// `rows`, read as [`columns_to_read`](Self::columns_to_read) lists them
    /// for `columns`, as a struct of every column of `columns` in order, each
    /// pinned one a constant of its code.
    pub(crate) fn fill_rows(&self, rows: ArrayRef, columns: &[&'static str]) -> Result<ArrayRef> {
        use vortex_array::arrays::ConstantArray;
        use vortex_array::arrays::struct_::StructArrayExt;

        if columns.iter().all(|&c| self.code(c).is_none()) {
            return Ok(rows);
        }
        let len = rows.len();
        let read = crate::store::array::into_struct_array(rows)?;
        let children = columns
            .iter()
            .map(|&name| match self.code(name) {
                Some(code) => Ok(ConstantArray::new(Scalar::from(code), len).into_array()),
                None => read
                    .unmasked_field_by_name(name)
                    .cloned()
                    .map_err(VortexRdfError::Vortex),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(named_struct(columns, children, len)?.into_array())
    }

    /// The `(s, p, o, g)` code columns of `kept` rows: each pinned one its
    /// code repeated, each other one its buffer among `codes`, read in
    /// `read_columns`' order.
    pub(crate) fn fill_codes(
        &self,
        read_columns: &[&'static str],
        mut codes: Vec<Buffer<TermCode>>,
        kept: usize,
    ) -> Result<[Buffer<TermCode>; 4]> {
        use crate::store::schema::{COL_G, COL_O, COL_P, COL_S};

        let mut column = |name: &str| match self.code(name) {
            Some(code) => Some(Buffer::full(code, kept)),
            None => {
                let at = read_columns.iter().position(|&c| c == name)?;
                codes.get_mut(at).map(std::mem::take)
            }
        };
        let filled = (|| {
            Some([
                column(COL_S)?,
                column(COL_P)?,
                column(COL_O)?,
                column(COL_G)?,
            ])
        })();
        filled.ok_or_else(|| {
            let missing = [COL_S, COL_P, COL_O, COL_G]
                .into_iter()
                .find(|&name| self.code(name).is_none() && !read_columns.contains(&name))
                .unwrap_or_default();
            VortexRdfError::InvalidOperation(format!(
                "a read neither read nor pinned the {missing} column"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::expr::and;

    /// Only a conjunction of `field == literal` over root fields decodes to
    /// `(column, code)` pairs; any other shape declines.
    #[test]
    fn code_eqs_accept_only_eq_conjunctions() {
        let filter = and(
            eq(get_item("p", root()), lit(3u32)),
            eq(get_item("o", root()), lit(7u32)),
        );
        let eqs = CodeEqs::of(&filter).unwrap();
        let pairs: Vec<(&str, u64)> = eqs.iter().map(|(c, v)| (c.as_ref(), v)).collect();
        assert_eq!(pairs, [("p", 3), ("o", 7)]);
        assert_eq!(
            (eqs.len(), eqs.code("o"), eqs.code("s")),
            (2, Some(7), None)
        );
        assert!(CodeEqs::of(&lit(false)).is_none());
        assert!(CodeEqs::of(&eq(get_item("p", root()), lit("x"))).is_none());
        let pushed = PushedFilter::new(filter);
        assert_eq!(pushed.eqs().map(CodeEqs::len), Some(2));
        assert!(PushedFilter::new(lit(false)).eqs().is_none());
    }

    /// A point read fills the equalities from `PIN_POINT_MIN_ROWS` rows.
    #[test]
    fn code_eqs_pin_point_reads_of_enough_rows() {
        let eqs = CodeEqs::of(&eq(get_item("p", root()), lit(3u32))).unwrap();
        assert!(eqs.for_points(PIN_POINT_MIN_ROWS - 1).is_empty());
        assert_eq!(eqs.for_points(PIN_POINT_MIN_ROWS).code("p"), Some(3));
        assert!(CodeEqs::NONE.for_points(usize::MAX).is_empty());
    }
}
