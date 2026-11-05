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

/// `x = c` is false for every row of a zone whose recorded values
/// (`vortexrdf.value_counts`, written for the graph columns) lack `c`:
/// `not(list_contains(get_item("values", stat(x, value_counts)), c))`, built-in
/// functions only, so Vortex's own pruning applies it in every filtered scan.
/// A zone past the aggregate's limit records null, and a column without the
/// statistic has none in scope: both leave the proof undecided. Given only
/// for a non-nullable `u64` column (the aggregate's input) and a non-null
/// `u64` literal, and not while `VORTEX_RDF_ZONES=0` ([`zones_switch`]).
#[derive(Debug)]
pub(crate) struct ValueCountsFalsifier;

impl vortex_array::stats::rewrite::StatsRewriteRule for ValueCountsFalsifier {
    fn scalar_fn_id(&self) -> ScalarFnId {
        use vortex_array::scalar_fn::ScalarFnVTable as _;
        vortex_array::scalar_fn::fns::binary::Binary.id()
    }

    fn falsify(
        &self,
        expr: &BoundExpression,
        _session: &VortexSession,
    ) -> VortexResult<Option<BoundExpression>> {
        use vortex_array::dtype::{DType, Nullability, PType};
        use vortex_array::expr::bound::{get_item, list_contains, not};
        use vortex_array::scalar_fn::fns::binary::Binary;
        use vortex_array::scalar_fn::fns::literal::Literal;
        use vortex_array::scalar_fn::fns::operators::Operator;
        use vortex_array::stats::expr::bound::stat;

        if !zones_switch() || !matches!(expr.as_::<Binary>(), Operator::Eq) {
            return Ok(None);
        }
        let (lhs, rhs) = (expr.child(0), expr.child(1));
        let (column, literal) = match (lhs.is::<Literal>(), rhs.is::<Literal>()) {
            (false, true) => (lhs, rhs),
            (true, false) => (rhs, lhs),
            _ => return Ok(None),
        };
        let u64 = DType::Primitive(PType::U64, Nullability::NonNullable);
        if column.dtype() != &u64 || literal.dtype() != &u64 || literal.as_::<Literal>().is_null() {
            return Ok(None);
        }
        let recorded = stat(column.clone(), crate::store::value_counts::value_counts());
        Ok(Some(not(list_contains(
            get_item("values", recorded),
            literal.clone(),
        ))))
    }
}

/// Whether zone decisions are on: `VORTEX_RDF_ZONES=0` (an ablation switch,
/// read once) turns them off, the [`ValueCountsFalsifier`] with them. The
/// process switch alone: Vortex may evaluate a rule on its own threads,
/// where a test's per-thread override doesn't reach.
pub(crate) fn zones_switch() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("VORTEX_RDF_ZONES").as_deref() != Ok("0"))
}

/// Register the store's rules in `session`.
pub(crate) fn register(session: &VortexSession) {
    use vortex_array::stats::session::StatsSessionExt as _;
    session.stats().register_rewrite(EqSatisfier);
    session.stats().register_rewrite(ValueCountsFalsifier);
}

/// The two proofs of `x = code` over a column of `column` dtype, as Vortex
/// builds them from the store session's rules: `none` holds where no row can
/// equal `code` (the falsifier), `all` where every row does (the
/// satisfier); either is `None` where no rule proves it.
#[cfg(feature = "file-io")]
pub(crate) struct EqProofs {
    pub(crate) none: Option<BoundExpression>,
    pub(crate) all: Option<BoundExpression>,
}

/// [`EqProofs`] of `x = code`, `x` a column of `column` dtype (the root of a
/// zone map's stats).
#[cfg(feature = "file-io")]
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
