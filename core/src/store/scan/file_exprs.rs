//! The expressions file reads push down over the quad table: the bind-memo
//! scope of its schema, the filter a pattern compiles to, and the code
//! equalities read back out of such a filter.

use vortex_array::expr::forms::conjuncts;
use vortex_array::expr::{Expression, and_collect, eq, get_item, lit, root};
use vortex_array::scalar::Scalar;

use crate::error::Result;
use crate::store::layouts::{Constraints, PatternCodes, QuadPattern};

/// The bind-memo scope tag for expressions over the quad table's schema
/// (the transparent root the file scan reads).
pub(crate) const QUAD_SCOPE: &str = "quads";

/// The pushed-down filter for a pattern under `codes`' layout: `AND` of
/// `column == code` per compiled constraint, `lit(false)` for an unmatchable
/// pattern, `None` when nothing is bound.
pub(crate) fn build_file_filter(
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes<'_>,
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

/// The `(column, code)` pairs of a filter [`build_file_filter`] built, a
/// conjunction of `column == literal` over root fields. `None` for any other
/// shape (the `AlwaysFalse` literal, say): such a filter cannot be applied as
/// per-row equality tests.
pub(crate) fn eq_code_pairs(filter: &Expression) -> Option<Vec<(String, u64)>> {
    use vortex_array::scalar_fn::fns::binary::Binary;
    use vortex_array::scalar_fn::fns::get_item::GetItem;
    use vortex_array::scalar_fn::fns::literal::Literal;
    use vortex_array::scalar_fn::fns::operators::Operator;

    conjuncts(filter)
        .iter()
        .map(|c| {
            let op = c.as_opt::<Binary>()?;
            if *op != Operator::Eq {
                return None;
            }
            let field = c.child(0).as_opt::<GetItem>()?;
            let scalar = c.child(1).as_opt::<Literal>()?;
            let code = u64::try_from(scalar).ok()?;
            Some((field.to_string(), code))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::expr::and;

    /// Only a conjunction of `field == literal` over root fields decodes to
    /// `(column, code)` pairs; any other shape declines.
    #[test]
    fn eq_code_pairs_accepts_only_eq_conjunctions() {
        let filter = and(
            eq(get_item("p", root()), lit(3u32)),
            eq(get_item("o", root()), lit(7u32)),
        );
        assert_eq!(
            eq_code_pairs(&filter),
            Some(vec![("p".to_string(), 3), ("o".to_string(), 7)])
        );
        assert!(eq_code_pairs(&lit(false)).is_none());
        assert!(eq_code_pairs(&eq(get_item("p", root()), lit("x"))).is_none());
    }
}
