//! The view model: where a view's rows come from ([`source`]), which rows it
//! covers ([`selection`]), the live canonical form of an encoded base
//! ([`canonical`]) and the per-array probe caches ([`probes`]).
pub(crate) mod canonical;
pub(crate) mod probes;
pub(crate) mod selection;
pub(crate) mod source;
