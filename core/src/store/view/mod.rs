//! The view model: where a view's rows come from ([`source`]), which rows it
//! covers ([`selection`]), the live canonical form of an encoded base
//! ([`canonical`]), the per-array probe caches ([`probes`]), the orders rows
//! come in ([`order`]) and what a planner may assume about a view
//! ([`stats`]).
pub(crate) mod canonical;
pub(crate) mod order;
pub(crate) mod probes;
pub(crate) mod selection;
pub(crate) mod source;
pub(crate) mod stats;
