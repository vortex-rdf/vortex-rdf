//! The statistics rule the store adds to Vortex's own, through its
//! extension point for them (`StatsRewriteRule`): a falsifier of `x = c`
//! from the values a graph column's zones record, which a session evaluates
//! against zone maps beside the built-in rules, so Vortex's own pruning
//! applies it in every filtered scan; and the switches that turn the store's
//! zone decisions off (`VORTEX_RDF_ZONES=0`).

use vortex_array::expr::BoundExpression;
use vortex_array::scalar_fn::ScalarFnId;
use vortex_error::VortexResult;
use vortex_session::VortexSession;

/// `x = c` is false for every row of a zone whose recorded values
/// (`vortexrdf.value_counts`, written for the graph columns) lack `c`:
/// `not(list_contains(get_item("values", stat(x, value_counts)), c))`, built-in
/// functions only, so Vortex's own pruning applies it in every filtered scan.
/// A zone past the aggregate's limit records null, and a column without the
/// statistic has none in scope: both leave the proof undecided. Given only
/// for a non-nullable `u64` column (the aggregate's input) and a non-null
/// `u64` literal, and not while `VORTEX_RDF_ZONES=0` ([`zones_switch`]).
/// A located run and the in-memory residual apply the same records
/// themselves, decoded once
/// ([`ColumnZones::eq_holds`](crate::zone_stats::zone_map::ColumnZones::eq_holds)).
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
        let recorded = stat(
            column.clone(),
            crate::zone_stats::value_counts::value_counts(),
        );
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

/// Whether reads decide their checked rows by the statistics: located runs
/// zone by zone, and the in-memory residual by its columns' zone maps.
/// `VORTEX_RDF_ZONES=0` ([`zones_switch`]) puts every read on its
/// row-by-row route; a test may force either for its thread
/// ([`ForcedZones`]).
///
/// [`ForcedZones`]: crate::store::test_hooks::ForcedZones
pub(crate) fn zones_enabled() -> bool {
    #[cfg(test)]
    if let Some(on) = crate::store::test_hooks::forced_zones() {
        return on;
    }
    zones_switch()
}

/// Register the store's rules in `session`.
pub(crate) fn register(session: &VortexSession) {
    use vortex_array::stats::session::StatsSessionExt as _;
    session.stats().register_rewrite(ValueCountsFalsifier);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A test forces the switch for its thread; the guard's drop restores it.
    #[test]
    fn forced_zones_override() {
        use crate::store::test_hooks::ForcedZones;

        let production = zones_enabled();
        {
            let _off = ForcedZones::set(false);
            assert!(!zones_enabled());
            let _on = ForcedZones::set(true);
            assert!(zones_enabled());
        }
        assert_eq!(zones_enabled(), production);
    }

    /// The store session's falsifier of `x = c`, which Vortex's scans prune
    /// with, carries the values a zone records beside its bounds, unless
    /// `VORTEX_RDF_ZONES=0` turns it off; a session without the store's
    /// rule has the bounds' alone.
    #[test]
    fn store_session_falsifier_carries_recorded_graphs() {
        use vortex_array::dtype::{DType, Nullability, PType};
        use vortex_array::expr::{eq, lit, root};

        let x = eq(root(), lit(5u64))
            .bind(&DType::Primitive(PType::U64, Nullability::NonNullable))
            .unwrap();
        let store = x
            .falsify(&crate::session::VORTEX_SESSION)
            .unwrap()
            .expect("the store session's falsifier");
        assert_eq!(
            store.to_string().contains("vortexrdf.value_counts"),
            zones_switch(),
            "{store}"
        );
        let bounds = x
            .falsify(&VortexSession::empty())
            .unwrap()
            .expect("Vortex's own falsifier");
        assert!(!bounds.to_string().contains("value_counts"), "{bounds}");
    }
}
