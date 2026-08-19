//! Patch lookup for bit-packed nodes.

use crate::node::Node;

/// Exception values of a bit-packed array, resolved as probe nodes: `indices`
/// holds strictly increasing patched positions (searchable), `values` the
/// replacement values (point access only, so its sort order never matters).
pub(crate) struct PatchProbe<'a> {
    pub(crate) indices: Box<Node<'a>>,
    pub(crate) values: Box<Node<'a>>,
    pub(crate) offset: usize,
}

impl PatchProbe<'_> {
    /// Replacement value for logical index `i`, if that position is patched.
    pub(crate) fn lookup(&self, i: usize) -> Option<u64> {
        let key = (i + self.offset) as u64;
        let at = self.indices.lower_bound(key);
        (at < self.indices.len() && self.indices.value_at(at) == key)
            .then(|| self.values.value_at(at))
    }
}
