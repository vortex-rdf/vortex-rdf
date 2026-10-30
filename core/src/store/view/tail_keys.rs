//! A tail's row keys ([`TailKeys`]): each row's values in the columns every
//! fully bound pattern constrains, hashed, in sorted runs. A fully bound
//! match on the tail (every `contains`, so every presence check `add_quads`
//! makes, and every `delete_quad`) reads the rows its key names instead of
//! comparing every row of the tail. Rows are keyed as they join the tail: an
//! append keys only its own rows, so a loop of single appends looks each quad
//! up in a handful of runs rather than scanning a tail that grows with every
//! append.

use std::hash::Hasher;
use std::sync::Arc;

use rustc_hash::FxHasher;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, VortexSessionExecute, match_each_unsigned_integer_ptype};

use crate::error::Result;
use crate::session::VORTEX_SESSION;
use crate::store::array::{StrColReader, field_as, unsigned_value};
use crate::store::layouts::ResolvedLayout;
use crate::store::schema::{COL_G, COL_O, COL_O_KIND, COL_O_VALUE, COL_P, COL_S, RowId};

/// One run of keys: `(key, tail row)` pairs, ascending.
type Run = Arc<[(u64, RowId)]>;

/// The row keys of a tail: for each row, a hash of its values in the key
/// columns of the tail's layout ([`key_columns`]), held in sorted runs over
/// consecutive rows, each under half the length of the run before it, so a
/// lookup binary-searches a handful of runs and an append sorts only its own
/// rows (and merges a run when it grows to half the one before).
///
/// A key names candidates: the match checks each row a key names against
/// the pattern's equalities, so two rows that share a key cost a check, never
/// a wrong answer.
#[derive(Clone)]
pub(crate) struct TailKeys {
    /// The tail layout's key columns.
    columns: &'static [&'static str],
    /// The sorted runs, oldest (and longest) first.
    runs: Vec<Run>,
    /// How many tail rows are keyed: rows `0..rows`.
    rows: usize,
}

impl TailKeys {
    /// The keys of every row of `rows`, a tail in `layout`; `None` under a
    /// layout no tail takes (see [`key_columns`]).
    pub(crate) fn of(rows: &ArrayRef, layout: &ResolvedLayout) -> Result<Option<Self>> {
        let Some(columns) = key_columns(layout) else {
            return Ok(None);
        };
        let empty = Self {
            columns,
            runs: Vec::new(),
            rows: 0,
        };
        empty.extended(rows).map(Some)
    }

    /// These keys and those of `fresh`, rows that follow the keyed ones in
    /// the tail: `fresh`'s keys sorted into a run of their own, merged into
    /// the run before while they hold at least half its length.
    pub(crate) fn extended(&self, fresh: &ArrayRef) -> Result<Self> {
        let start = self.rows as RowId;
        let mut run: Vec<(u64, RowId)> = row_keys(fresh, self.columns)?
            .into_iter()
            .zip(start..)
            .collect();
        run.sort_unstable();
        let mut runs = self.runs.clone();
        while let Some(last) = runs.last()
            && run.len() * 2 >= last.len()
        {
            run = merged(last, &run);
            runs.pop();
        }
        if !run.is_empty() {
            runs.push(run.into());
        }
        Ok(Self {
            columns: self.columns,
            runs,
            rows: self.rows + fresh.len(),
        })
    }

    /// The tail rows that may match `eqs`, a pattern's equalities, ascending:
    /// those whose key is the one `eqs` give the key columns. `None` when
    /// `eqs` leave a key column unconstrained (a pattern not fully bound),
    /// which no key can answer.
    pub(crate) fn candidates(&self, eqs: &[(&'static str, Scalar)]) -> Option<Vec<RowId>> {
        let mut hasher = FxHasher::default();
        for column in self.columns {
            let (_, value) = eqs.iter().find(|(name, _)| name == column)?;
            let value = match unsigned_value(value) {
                Some(n) => KeyValue::Num(n),
                None => KeyValue::Str(value.as_utf8_opt()?.value()?.as_bytes()),
            };
            value.hash_into(&mut hasher);
        }
        let key = hasher.finish();
        let mut rows: Vec<RowId> = self
            .runs
            .iter()
            .flat_map(|run| {
                let from = run.partition_point(|&(k, _)| k < key);
                run[from..]
                    .iter()
                    .take_while(move |&&(k, _)| k == key)
                    .map(|&(_, row)| row)
            })
            .collect();
        rows.sort_unstable();
        Some(rows)
    }

    /// How many rows are keyed.
    #[cfg(test)]
    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    /// The length of each run, oldest first.
    #[cfg(test)]
    pub(crate) fn run_lengths(&self) -> Vec<usize> {
        self.runs.iter().map(|run| run.len()).collect()
    }
}

/// The columns of a tail in `layout` that every fully bound pattern
/// constrains (`PatternCodes::constraints` compiles one equality per bound
/// role): under the Default layout the four term strings; under TypedObject
/// the subject, predicate and graph strings with the object's kind and value,
/// its datatype and language being constrained only for the literals that
/// have one (the check after a lookup compares them). `None` under the
/// Dictionary layout, which no tail takes: a tail stores strings
/// (`Tail::layout_for`).
fn key_columns(layout: &ResolvedLayout) -> Option<&'static [&'static str]> {
    match layout {
        ResolvedLayout::Default => Some(&[COL_S, COL_P, COL_O, COL_G]),
        ResolvedLayout::TypedObject => Some(&[COL_S, COL_P, COL_O_KIND, COL_O_VALUE, COL_G]),
        ResolvedLayout::Dictionary(_) => None,
    }
}

/// A key column's value, read the same way off a row and off a pattern's
/// equality.
enum KeyValue<'a> {
    Str(&'a [u8]),
    Num(u64),
}

impl KeyValue<'_> {
    fn hash_into(&self, hasher: &mut FxHasher) {
        match self {
            KeyValue::Str(bytes) => {
                hasher.write_u8(0);
                hasher.write_u64(bytes.len() as u64);
                hasher.write(bytes);
            }
            KeyValue::Num(n) => {
                hasher.write_u8(1);
                hasher.write_u64(*n);
            }
        }
    }
}

/// The key of each row of `rows`, a struct of the tail's columns, in row
/// order: the hash of its values in `columns`, unsigned integers (the
/// object's kind) as numbers and anything else as the string column it is.
fn row_keys(rows: &ArrayRef, columns: &[&'static str]) -> Result<Vec<u64>> {
    #[cfg(test)]
    key_hooks::note_keyed(rows.len());
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let rows = rows.clone().execute::<StructArray>(&mut ctx)?;
    let mut hashers = vec![FxHasher::default(); rows.len()];
    for &name in columns {
        let field = rows.unmasked_field_by_name(name)?;
        if field.dtype().is_unsigned_int() {
            let values = field.clone().execute::<PrimitiveArray>(&mut ctx)?;
            match_each_unsigned_integer_ptype!(values.ptype(), |T| {
                for (hasher, &n) in hashers.iter_mut().zip(values.as_slice::<T>()) {
                    // The identity in the macro's `u64` arm.
                    #[allow(clippy::unnecessary_cast)]
                    let n = n as u64;
                    KeyValue::Num(n).hash_into(hasher);
                }
            });
        } else {
            let strings = field_as::<VarBinViewArray>(&rows, name, &mut ctx)?;
            let reader = StrColReader::new(&strings);
            for (row, hasher) in hashers.iter_mut().enumerate() {
                KeyValue::Str(reader.bytes_at(row)).hash_into(hasher);
            }
        }
    }
    Ok(hashers.into_iter().map(|hasher| hasher.finish()).collect())
}

/// The ascending merge of two ascending runs.
fn merged(older: &[(u64, RowId)], newer: &[(u64, RowId)]) -> Vec<(u64, RowId)> {
    let mut out = Vec::with_capacity(older.len() + newer.len());
    let (mut i, mut j) = (0, 0);
    while i < older.len() && j < newer.len() {
        if older[i] <= newer[j] {
            out.push(older[i]);
            i += 1;
        } else {
            out.push(newer[j]);
            j += 1;
        }
    }
    out.extend_from_slice(&older[i..]);
    out.extend_from_slice(&newer[j..]);
    out
}

/// Test hook: how many tail rows this thread keyed.
#[cfg(test)]
pub(crate) mod key_hooks {
    use std::cell::Cell;

    thread_local! {
        static KEYED: Cell<usize> = const { Cell::new(0) };
    }

    /// Record `rows` rows keyed.
    pub(super) fn note_keyed(rows: usize) {
        KEYED.set(KEYED.get() + rows);
    }

    /// The tail rows keyed on this thread so far.
    pub(crate) fn keyed_rows() -> usize {
        KEYED.get()
    }
}
