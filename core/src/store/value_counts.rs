//! The `vortexrdf.value_counts` zone aggregate: each zone's distinct values
//! with their row counts, while the zone holds at most `max_values` of them
//! (null past that). Its state is a struct of two lists, `values` (u64,
//! ascending) and their `counts` (u32), so Vortex's built-in `list_contains`
//! can test whether a zone holds a value (the store's falsifier of `g = c`).
//! The store writes it into the zone maps of its graph columns, and a build
//! counts an in-memory graph column's zones with the same [`Counts`], in the
//! pass that compresses the column.

use std::fmt::{self, Display, Formatter};

use vortex_array::aggregate_fn::{
    AggregateArgs, AggregateFnId, AggregateFnRef, AggregateFnVTable, AggregateFnVTableExt as _,
};
use vortex_array::dtype::{DType, Nullability, PType};
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, Canonical, Columnar, ExecutionCtx};
use vortex_error::{VortexResult, vortex_bail};
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

/// The aggregate's id, as zone maps and editions name it.
pub(crate) const VALUE_COUNTS_ID: &str = "vortexrdf.value_counts";

/// Distinct values a zone records at most.
pub(crate) const MAX_VALUES: u32 = 16;

/// The `vortexrdf.value_counts` aggregate.
#[derive(Clone, Debug)]
pub(crate) struct ValueCounts;

/// Its one option: the distinct values a zone records at most.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ValueCountsOptions {
    pub(crate) max_values: u32,
}

impl Display for ValueCountsOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.max_values)
    }
}

/// A zone's counts so far: `(value, count)` ascending by value, or too many
/// values to record.
#[derive(Clone, Debug)]
pub(crate) enum Counts {
    Values(Vec<(u64, u64)>),
    TooMany,
}

impl Counts {
    /// A zone's counts before its first row.
    pub(crate) fn new() -> Self {
        Counts::Values(Vec::new())
    }

    fn add(&mut self, value: u64, n: u64, max: u32) {
        let Counts::Values(values) = self else {
            return;
        };
        match values.binary_search_by_key(&value, |&(v, _)| v) {
            Ok(at) => values[at].1 += n,
            Err(at) => {
                if values.len() >= max as usize {
                    *self = Counts::TooMany;
                } else {
                    values.insert(at, (value, n));
                }
            }
        }
    }

    /// Count `values` in, a run of one value at a time (graph columns come
    /// in runs), at most `max` values recorded.
    pub(crate) fn add_all(&mut self, values: &[u64], max: u32) {
        let mut at = 0;
        while at < values.len() && !matches!(self, Counts::TooMany) {
            let value = values[at];
            let run = values[at..].iter().take_while(|&&v| v == value).count();
            self.add(value, run as u64, max);
            at += run;
        }
    }

    /// The zone's `(value, rows)` pairs, ascending by value, as its state
    /// records them ([`decode_state`] of the state the aggregate writes):
    /// `None` past `max_values` values, and for a count past a `u32`'s
    /// range, more than a state holds.
    pub(crate) fn recorded(&self) -> Option<Box<[(u64, u32)]>> {
        let Counts::Values(values) = self else {
            return None;
        };
        values
            .iter()
            .map(|&(value, n)| Some((value, u32::try_from(n).ok()?)))
            .collect()
    }
}

/// A list element of the state: a non-nullable `ptype`.
fn element(ptype: PType) -> DType {
    DType::Primitive(ptype, Nullability::NonNullable)
}

/// The aggregate's state: a nullable struct of `values` (u64) and their
/// `counts` (u32), both lists non-nullable.
pub(crate) fn state_dtype() -> DType {
    use vortex_array::dtype::StructFields;
    DType::Struct(
        StructFields::from_iter([
            (
                "values",
                DType::List(
                    std::sync::Arc::new(element(PType::U64)),
                    Nullability::NonNullable,
                ),
            ),
            (
                "counts",
                DType::List(
                    std::sync::Arc::new(element(PType::U32)),
                    Nullability::NonNullable,
                ),
            ),
        ]),
        Nullability::Nullable,
    )
}

/// The aggregate as the store writes and reads it: [`ValueCounts`] bound
/// with [`MAX_VALUES`].
pub(crate) fn value_counts() -> AggregateFnRef {
    ValueCounts.bind(ValueCountsOptions {
        max_values: MAX_VALUES,
    })
}

/// A recorded state's `(value, count)` pairs, ascending by value; `None` for
/// a zone that held more than `max_values` values (a null state), and for a
/// state the aggregate does not write ([`pairs`]).
pub(crate) fn decode_state(state: &Scalar) -> Option<Vec<(u64, u32)>> {
    if state.is_null() {
        return None;
    }
    let state = state.as_struct();
    let list = |name: &str| {
        state
            .field(name)
            .and_then(|field| field.as_list().elements())
    };
    let values: Vec<u64> = list("values")?
        .iter()
        .map(|value| {
            value
                .as_primitive()
                .typed_value::<u64>()
                .expect("a u64 value")
        })
        .collect();
    let counts: Vec<u32> = list("counts")?
        .iter()
        .map(|count| {
            count
                .as_primitive()
                .typed_value::<u32>()
                .expect("a u32 count")
        })
        .collect();
    pairs(&values, &counts)
}

/// The state recording `values` with their `counts`, as given.
pub(crate) fn state_of(values: &[u64], counts: &[u32]) -> Scalar {
    let values = values
        .iter()
        .map(|&value| Scalar::primitive(value, Nullability::NonNullable))
        .collect();
    let counts = counts
        .iter()
        .map(|&n| Scalar::primitive(n, Nullability::NonNullable))
        .collect();
    let list = |ptype, items| {
        Scalar::list(
            std::sync::Arc::new(element(ptype)),
            items,
            Nullability::NonNullable,
        )
    };
    Scalar::struct_(
        state_dtype(),
        [list(PType::U64, values), list(PType::U32, counts)],
    )
}

/// A state's `values` paired with their `counts`, where the two make a state
/// the aggregate writes: as many counts as values, the values strictly
/// ascending. `None` otherwise, for a state another writer could leave.
pub(crate) fn pairs(values: &[u64], counts: &[u32]) -> Option<Vec<(u64, u32)>> {
    (values.len() == counts.len() && values.is_sorted_by(|a, b| a < b))
        .then(|| values.iter().copied().zip(counts.iter().copied()).collect())
}

impl AggregateFnVTable for ValueCounts {
    type Options = ValueCountsOptions;
    type Partial = Counts;

    fn id(&self) -> AggregateFnId {
        static ID: CachedId = CachedId::new(VALUE_COUNTS_ID);
        *ID
    }

    fn serialize(&self, options: &Self::Options) -> VortexResult<Option<Vec<u8>>> {
        Ok(Some(options.max_values.to_le_bytes().to_vec()))
    }

    fn deserialize(
        &self,
        metadata: &[u8],
        _session: &VortexSession,
    ) -> VortexResult<Self::Options> {
        let Ok(bytes) = <[u8; 4]>::try_from(metadata) else {
            vortex_bail!(
                "{VALUE_COUNTS_ID} options are 4 bytes, not {}",
                metadata.len()
            );
        };
        Ok(ValueCountsOptions {
            max_values: u32::from_le_bytes(bytes),
        })
    }

    fn return_dtype(&self, _options: &Self::Options, input_dtype: &DType) -> Option<DType> {
        matches!(
            input_dtype,
            DType::Primitive(PType::U64, Nullability::NonNullable)
        )
        .then(state_dtype)
    }

    fn partial_dtype(&self, options: &Self::Options, input_dtype: &DType) -> Option<DType> {
        self.return_dtype(options, input_dtype)
    }

    fn empty_partial(
        &self,
        _args: AggregateArgs<'_, Self::Options>,
    ) -> VortexResult<Self::Partial> {
        Ok(Counts::new())
    }

    fn partial_from_scalar(
        &self,
        _args: AggregateArgs<'_, Self::Options>,
        scalar: &Scalar,
    ) -> VortexResult<Self::Partial> {
        Ok(match decode_state(scalar) {
            None => Counts::TooMany,
            Some(pairs) => Counts::Values(
                pairs
                    .into_iter()
                    .map(|(value, n)| (value, u64::from(n)))
                    .collect(),
            ),
        })
    }

    fn merge_partials(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        first: Self::Partial,
        second: Self::Partial,
    ) -> VortexResult<Self::Partial> {
        let (Counts::Values(merged), Counts::Values(second)) = (first, second) else {
            return Ok(Counts::TooMany);
        };
        let mut out = Counts::Values(merged);
        for (value, n) in second {
            out.add(value, n, args.options.max_values);
        }
        Ok(out)
    }

    fn to_scalar(
        &self,
        _args: AggregateArgs<'_, Self::Options>,
        partial: &Self::Partial,
    ) -> VortexResult<Scalar> {
        let Some(recorded) = partial.recorded() else {
            return Ok(Scalar::null(state_dtype()));
        };
        let values: Vec<u64> = recorded.iter().map(|&(value, _)| value).collect();
        let counts: Vec<u32> = recorded.iter().map(|&(_, n)| n).collect();
        Ok(state_of(&values, &counts))
    }

    fn is_saturated(
        &self,
        _args: AggregateArgs<'_, Self::Options>,
        partial: &Self::Partial,
    ) -> bool {
        matches!(partial, Counts::TooMany)
    }

    fn accumulate(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        partial: &mut Self::Partial,
        batch: &Columnar,
        _ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        let max = args.options.max_values;
        match batch {
            Columnar::Constant(c) => match c.scalar().as_primitive().typed_value::<u64>() {
                Some(value) => partial.add(value, c.len() as u64, max),
                // Rows without a value to count leave the zone unrecorded.
                None => *partial = Counts::TooMany,
            },
            Columnar::Canonical(Canonical::Primitive(p)) => {
                partial.add_all(p.as_slice::<u64>(), max);
            }
            Columnar::Canonical(_) => {
                vortex_bail!(
                    "{VALUE_COUNTS_ID} counts u64 columns, not {}",
                    batch.dtype()
                )
            }
        }
        Ok(())
    }

    fn finalize(
        &self,
        _args: AggregateArgs<'_, Self::Options>,
        states: ArrayRef,
    ) -> VortexResult<ArrayRef> {
        Ok(states)
    }

    fn finalize_scalar(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        partial: &Self::Partial,
    ) -> VortexResult<Scalar> {
        self.to_scalar(args, partial)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::VORTEX_SESSION;
    use vortex_array::aggregate_fn::{Accumulator, AggregateArgs, DynAccumulator as _};
    use vortex_array::arrays::{ConstantArray, PrimitiveArray};
    use vortex_array::dtype::{DType, Nullability, PType};
    use vortex_array::{ArrayRef, IntoArray as _, VortexSessionExecute as _};

    const U64: DType = DType::Primitive(PType::U64, Nullability::NonNullable);

    fn options() -> ValueCountsOptions {
        ValueCountsOptions {
            max_values: MAX_VALUES,
        }
    }

    fn values(values: impl IntoIterator<Item = u64>) -> ArrayRef {
        PrimitiveArray::from_iter(values).into_array()
    }

    /// The state the aggregate records over `batches`, decoded.
    fn recorded(batches: &[ArrayRef]) -> Option<Vec<(u64, u32)>> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let mut acc = Accumulator::try_new(ValueCounts, options(), U64).unwrap();
        for batch in batches {
            acc.accumulate(batch, &mut ctx).unwrap();
        }
        decode_state(&acc.finish().unwrap())
    }

    #[test]
    fn records_each_value_with_its_count() {
        assert_eq!(
            recorded(&[values([5, 5, 3, 9, 3, 3])]),
            Some(vec![(3, 3), (5, 2), (9, 1)])
        );
    }

    #[test]
    fn constant_input_counts_its_length() {
        let constant = ConstantArray::new(7u64, 8192).into_array();
        assert_eq!(recorded(&[constant]), Some(vec![(7, 8192)]));
    }

    #[test]
    fn records_up_to_max_values() {
        let sixteen = recorded(&[values(0..16)]);
        assert_eq!(sixteen.map(|pairs| pairs.len()), Some(16));
        assert_eq!(recorded(&[values(0..17)]), None);
    }

    #[test]
    fn merges_partials() {
        let opts = options();
        let state = state_dtype();
        let args = || AggregateArgs {
            options: &opts,
            dtype: &U64,
            return_dtype: &state,
            partial_dtype: &state,
        };
        let partial = |batch: ArrayRef| {
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            let mut acc = Accumulator::try_new(ValueCounts, opts, U64).unwrap();
            acc.accumulate(&batch, &mut ctx).unwrap();
            let scalar = acc.partial_scalar().unwrap();
            ValueCounts.partial_from_scalar(args(), &scalar).unwrap()
        };
        let merged = ValueCounts
            .merge_partials(args(), partial(values([1, 2, 2])), partial(values([2, 3])))
            .unwrap();
        assert_eq!(
            decode_state(&ValueCounts.to_scalar(args(), &merged).unwrap()),
            recorded(&[values([1, 2, 2, 2, 3])])
        );
        let saturated = ValueCounts
            .merge_partials(args(), partial(values(0..17)), partial(values([1])))
            .unwrap();
        assert_eq!(
            decode_state(&ValueCounts.to_scalar(args(), &saturated).unwrap()),
            None
        );
    }

    /// A count past a u32's range is more than a state holds: the zone
    /// records nothing, not a count cut to 32 bits.
    #[test]
    fn a_count_past_u32_records_nothing() {
        let rows = usize::try_from(u64::from(u32::MAX) + 1).unwrap();
        let constant = ConstantArray::new(7u64, rows).into_array();
        assert_eq!(recorded(&[constant]), None);
    }

    /// Rows without a value to count (a null constant, which a non-nullable
    /// input never holds) leave the zone unrecorded, not recorded without
    /// them.
    #[test]
    fn a_null_constant_records_nothing() {
        use vortex_array::scalar::Scalar;

        let opts = options();
        let state = state_dtype();
        let args = || AggregateArgs {
            options: &opts,
            dtype: &U64,
            return_dtype: &state,
            partial_dtype: &state,
        };
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let mut partial = ValueCounts.empty_partial(args()).unwrap();
        let null = Scalar::null(DType::Primitive(PType::U64, Nullability::Nullable));
        let batch = Columnar::Constant(ConstantArray::new(null, 4));
        ValueCounts
            .accumulate(args(), &mut partial, &batch, &mut ctx)
            .unwrap();
        let scalar = ValueCounts.to_scalar(args(), &partial).unwrap();
        assert_eq!(decode_state(&scalar), None);
    }

    /// A state decodes to its pairs only as the aggregate writes it: as many
    /// counts as values, the values strictly ascending.
    #[test]
    fn decodes_only_well_formed_states() {
        let decoded = |values: &[u64], counts: &[u32]| decode_state(&state_of(values, counts));
        assert_eq!(decoded(&[1, 3], &[2, 5]), Some(vec![(1, 2), (3, 5)]));
        assert_eq!(decoded(&[], &[]), Some(vec![]));
        assert_eq!(decoded(&[1, 3], &[2]), None, "fewer counts");
        assert_eq!(decoded(&[1], &[2, 5]), None, "more counts");
        assert_eq!(decoded(&[3, 1], &[2, 5]), None, "descending");
        assert_eq!(decoded(&[3, 3], &[2, 5]), None, "repeated");
    }

    /// The field a zone table names the statistic by: the aggregate's id and
    /// its option, as Vortex's `ZoneMap` looks it up.
    #[test]
    fn zone_tables_name_it_with_its_option() {
        assert_eq!(value_counts().to_string(), "vortexrdf.value_counts(16)");
    }

    #[test]
    fn options_round_trip() {
        let bytes = ValueCounts.serialize(&options()).unwrap().unwrap();
        assert_eq!(
            ValueCounts.deserialize(&bytes, &VORTEX_SESSION).unwrap(),
            options()
        );
        let error = ValueCounts
            .deserialize(&[1, 2, 3], &VORTEX_SESSION)
            .unwrap_err();
        assert!(error.to_string().contains(VALUE_COUNTS_ID), "{error}");
    }

    #[test]
    fn accepts_only_non_nullable_u64() {
        assert_eq!(
            ValueCounts.return_dtype(&options(), &U64),
            Some(state_dtype())
        );
        for dtype in [
            DType::Primitive(PType::U32, Nullability::NonNullable),
            DType::Primitive(PType::U64, Nullability::Nullable),
            DType::Utf8(Nullability::NonNullable),
        ] {
            assert_eq!(
                ValueCounts.return_dtype(&options(), &dtype),
                None,
                "{dtype}"
            );
        }
    }
}
