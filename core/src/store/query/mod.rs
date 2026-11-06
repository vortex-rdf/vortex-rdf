//! Narrowing a store into views: pattern matching, keeps and windows, and
//! batched probes.

pub(crate) mod batch;
pub(crate) mod keep;
pub(crate) mod located;
pub(crate) mod matching;
pub(crate) mod partitions;
pub(crate) mod served;
pub(crate) mod window;
pub(crate) mod zone_decider;
