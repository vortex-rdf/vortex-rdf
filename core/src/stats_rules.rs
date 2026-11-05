//! Statistics proofs the store adds to Vortex's own, through its extension
//! point for them (`StatsRewriteRule`): rules a session evaluates against
//! zone maps and footer statistics like the built-in ones.

use vortex_array::expr::BoundExpression;
use vortex_array::scalar_fn::ScalarFnId;
use vortex_error::VortexResult;
use vortex_session::VortexSession;

/// `x = c` is true for every row of a scope where `x`'s minimum and maximum
/// there are both `c`: the satisfier Vortex 0.90 lacks for an equality (its
/// own rules satisfy literals and null tests only). Given only where it is
/// sound: `x` a non-nullable integer and `c` a non-null literal. A float's
/// NaNs and a nullable column's nulls fail `x = c` inside bounds of `c`.
#[derive(Debug)]
pub(crate) struct EqSatisfier;

impl vortex_array::stats::rewrite::StatsRewriteRule for EqSatisfier {
    fn scalar_fn_id(&self) -> ScalarFnId {
        use vortex_array::scalar_fn::ScalarFnVTable as _;
        vortex_array::scalar_fn::fns::binary::Binary.id()
    }

    fn satisfy(
        &self,
        expr: &BoundExpression,
        _session: &VortexSession,
    ) -> VortexResult<Option<BoundExpression>> {
        use vortex_array::expr::bound::{and, eq};
        use vortex_array::expr::stats::Stat;
        use vortex_array::scalar_fn::fns::binary::Binary;
        use vortex_array::scalar_fn::fns::literal::Literal;
        use vortex_array::scalar_fn::fns::operators::Operator;
        use vortex_array::stats::expr::bound::stat;

        if !matches!(expr.as_::<Binary>(), Operator::Eq) {
            return Ok(None);
        }
        let (lhs, rhs) = (expr.child(0), expr.child(1));
        let (column, literal) = match (lhs.is::<Literal>(), rhs.is::<Literal>()) {
            (false, true) => (lhs, rhs),
            (true, false) => (rhs, lhs),
            _ => return Ok(None),
        };
        if !column.dtype().is_int()
            || column.dtype().is_nullable()
            || literal.as_::<Literal>().is_null()
        {
            return Ok(None);
        }
        let (Some(min), Some(max)) = (Stat::Min.aggregate_fn(), Stat::Max.aggregate_fn()) else {
            return Ok(None);
        };
        Ok(Some(and(
            eq(stat(column.clone(), min), literal.clone()),
            eq(stat(column.clone(), max), literal.clone()),
        )))
    }
}

/// Register the store's rules in `session`.
pub(crate) fn register(session: &VortexSession) {
    use vortex_array::stats::session::StatsSessionExt as _;
    session.stats().register_rewrite(EqSatisfier);
}

/// The two proofs of `x = code` over a column of `column` dtype, as Vortex
/// builds them from the store session's rules: `none` holds where no row can
/// equal `code` (the falsifier), `all` where every row does (the
/// satisfier); either is `None` where no rule proves it.
#[cfg(feature = "file-io")]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "read by the zone decider from P1 task 5")
)]
pub(crate) struct EqProofs {
    pub(crate) none: Option<BoundExpression>,
    pub(crate) all: Option<BoundExpression>,
}

/// [`EqProofs`] of `x = code`, `x` a column of `column` dtype (the root of a
/// zone map's stats).
#[cfg(feature = "file-io")]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "read by the zone decider from P1 task 5")
)]
pub(crate) fn eq_proofs(
    column: &vortex_array::dtype::DType,
    code: u64,
) -> crate::error::Result<EqProofs> {
    use crate::session::VORTEX_SESSION;
    use vortex_array::expr::{eq, lit, root};

    let expr = eq(root(), lit(code)).bind(column)?;
    Ok(EqProofs {
        none: expr.falsify(&VORTEX_SESSION)?,
        all: expr.satisfy(&VORTEX_SESSION)?,
    })
}

#[cfg(all(test, feature = "file-io"))]
mod tests {
    use super::*;
    use crate::io::zone_map::ColumnZones;
    use vortex_array::dtype::{DType, Nullability, PType};

    const U64: DType = DType::Primitive(PType::U64, Nullability::NonNullable);

    fn bits(mask: vortex_mask::Mask) -> Vec<bool> {
        (0..mask.len()).map(|i| mask.value(i)).collect()
    }

    /// `x = 5` is proved for every row only where a zone's bounds are 5 and
    /// 5, and refuted where they leave 5 out.
    #[test]
    fn satisfier_holds_only_where_min_equals_max_equals_c() {
        let zones = ColumnZones::from_bounds(
            vortex_buffer::buffer![5u64, 4, 5, 6, 1],
            vortex_buffer::buffer![5u64, 5, 6, 7, 3],
            8_192,
            36_000,
        )
        .unwrap();
        let proofs = eq_proofs(&U64, 5).unwrap();
        let all = proofs.all.expect("a satisfier for a u64 column");
        let none = proofs.none.expect("a falsifier for a u64 column");
        assert_eq!(
            bits(zones.prune(0..5, &all).unwrap()),
            [true, false, false, false, false]
        );
        assert_eq!(
            bits(zones.prune(0..5, &none).unwrap()),
            [false, false, false, true, true]
        );
    }

    /// The satisfier at the top of the `u64` range.
    #[test]
    fn satisfier_at_the_top_of_u64() {
        let zones = ColumnZones::from_bounds(
            vortex_buffer::buffer![u64::MAX, u64::MAX - 1],
            vortex_buffer::buffer![u64::MAX, u64::MAX],
            8_192,
            16_384,
        )
        .unwrap();
        let all = eq_proofs(&U64, u64::MAX).unwrap().all.unwrap();
        assert_eq!(bits(zones.prune(0..2, &all).unwrap()), [true, false]);
    }

    /// No satisfier where min = max = c proves nothing: a float zone's NaNs
    /// and a nullable zone's nulls fail `x = c` at bounds of c.
    #[test]
    fn no_satisfier_for_floats_or_nullable() {
        use vortex_array::expr::{eq, lit, root};

        let float = DType::Primitive(PType::F64, Nullability::NonNullable);
        let satisfier = eq(root(), lit(5.0f64))
            .bind(&float)
            .unwrap()
            .satisfy(&crate::session::VORTEX_SESSION)
            .unwrap();
        assert!(satisfier.is_none(), "floats");
        let nullable = DType::Primitive(PType::U64, Nullability::Nullable);
        assert!(eq_proofs(&nullable, 5).unwrap().all.is_none(), "nullable");
    }
}
