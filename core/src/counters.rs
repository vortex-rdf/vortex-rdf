//! The store's work counters, for the ablation study: a located run's zone
//! decisions, a pair of counters per site (the decisions, then their rows).
//! They count under the encoded search's switch, `VORTEX_RDF_COUNTERS=1`,
//! read once per process ([`vortex_rdf_encoded_search::counters::on`]); off,
//! a note costs one relaxed load. The bindings report them after the encoded
//! search's own, as the engine counters ([`ENGINE_NAMES`],
//! [`engine_snapshot`]).

use std::sync::atomic::{AtomicU64, Ordering};

use vortex_rdf_encoded_search::counters as search;

/// The sites, each from one entry: its docs, its index and its name, which
/// also names its values' counter (`name_values`).
macro_rules! sites {
    ($($(#[doc = $doc:literal])+ $site:ident = $name:literal,)+) => {
        /// The sites in list order, which places their counters.
        #[allow(non_camel_case_types)]
        enum Site {
            $($site,)+
        }
        $(
            $(#[doc = $doc])+
            pub const $site: usize = 2 * Site::$site as usize;
        )+
        /// How many counters there are: a pair per site.
        pub const LEN: usize = 2 * [$($name),+].len();
        /// Names in [`snapshot`] order.
        pub const NAMES: [&str; LEN] = [$($name, concat!($name, "_values")),+];
    };
}

sites! {
    /// A located run's checked slot settled for the whole file by its footer
    /// statistics (values: the run's rows).
    ZONE_COLUMN = "zone_column",
    /// A located run's zone (its rows of it) settled as holding no match.
    ZONE_NONE = "zone_none",
    /// A located run's zone (its rows of it) settled as all matches, unread.
    ZONE_ALL = "zone_all",
    /// A located run's mixed zone counted on the checked column's encoding.
    ZONE_COUNTED = "zone_counted",
    /// A located run's rows read at checked columns to check them.
    CHECK_READ = "check_read",
    /// A located run's mixed zone counted from the checked column's recorded
    /// values (`vortexrdf.value_counts`), unread.
    ZONE_RECORDED = "zone_recorded",
}

static C: [AtomicU64; LEN] = [const { AtomicU64::new(0) }; LEN];

/// One decision at `site`, of `values` rows.
pub fn note(site: usize, values: u64) {
    if search::on() {
        C[site].fetch_add(1, Ordering::Relaxed);
        C[site + 1].fetch_add(values, Ordering::Relaxed);
    }
}

/// The counters, in [`NAMES`] order.
pub fn snapshot() -> [u64; LEN] {
    std::array::from_fn(|i| C[i].load(Ordering::Relaxed))
}

/// How many engine counters there are: the encoded search's, then the
/// store's.
pub const ENGINE_LEN: usize = search::LEN + LEN;

/// Names in [`engine_snapshot`] order: [`search::NAMES`], then [`NAMES`].
pub const ENGINE_NAMES: [&str; ENGINE_LEN] = {
    let mut names = [""; ENGINE_LEN];
    let mut i = 0;
    while i < ENGINE_LEN {
        names[i] = if i < search::LEN {
            search::NAMES[i]
        } else {
            NAMES[i - search::LEN]
        };
        i += 1;
    }
    names
};

/// The engine counters, in [`ENGINE_NAMES`] order, cumulative in this
/// process.
pub fn engine_snapshot() -> [u64; ENGINE_LEN] {
    let (searched, stored) = (search::snapshot(), snapshot());
    std::array::from_fn(|i| match i.checked_sub(search::LEN) {
        None => searched[i],
        Some(i) => stored[i],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_rdf_encoded_search::counters as search;

    /// The engine counters the bindings report are the encoded search's
    /// released 18, then the store's own, in the order they always had.
    #[test]
    fn engine_counters_keep_their_names_and_order() {
        assert_eq!(
            search::NAMES.len(),
            18,
            "the encoded search's released sites"
        );
        assert_eq!(ENGINE_NAMES[..18], search::NAMES);
        assert_eq!(
            ENGINE_NAMES[18..],
            [
                "zone_column",
                "zone_column_values",
                "zone_none",
                "zone_none_values",
                "zone_all",
                "zone_all_values",
                "zone_counted",
                "zone_counted_values",
                "check_read",
                "check_read_values",
                "zone_recorded",
                "zone_recorded_values",
            ]
        );
        assert_eq!(NAMES[ZONE_ALL], "zone_all");
        assert_eq!(NAMES[CHECK_READ + 1], "check_read_values");
        assert_eq!(engine_snapshot().len(), ENGINE_LEN);
    }
}
