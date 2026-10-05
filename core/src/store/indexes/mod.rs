//! The secondary-index hub: [`IndexType`] and its dispatch into the leaf
//! modules, the resolution currency (`IndexResolution`, `ResolvedRoles`,
//! `ResolvedRowIds`, `LazyRowIds`) both backends answer in, and the planner
//! loops that try a store's indexes in preference order.

use std::sync::{Arc, OnceLock};

use vortex_array::ArrayRef;
use vortex_buffer::Buffer;

use crate::error::{Result, VortexRdfError};
use crate::store::layouts::{PatternCodes, QuadPattern, ResolvedLayout};

pub(crate) mod components;
pub(crate) mod copy;
#[cfg(feature = "file-io")]
pub(crate) mod file;
pub(crate) mod reference;
pub(crate) mod resolve;
pub(crate) mod serve;

#[cfg(feature = "file-io")]
pub(crate) use components::check_component_rows;
pub(crate) use components::{
    ComponentIdentity, DeferredSource, IndexComponent, KnownComponent, adopt_component,
    component_named, indexes_from_components, known_component, sorted_row_ids,
};
#[cfg(feature = "file-io")]
pub(crate) use file::FileServePlan;
use resolve::IndexProbe;
pub(crate) use serve::InMemoryServePlan;

/// The primary-row-id column every index child carries. A component's rid
/// addresses rows of the base it was built against.
pub(crate) const COL_RID: &str = "rid";

/// A secondary index, built as its own sorted children beside the quad rows.
///
/// Variant order is the resolution preference order: pattern matching tries
/// the indexes a store carries in this order and takes the first that does
/// not decline.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum IndexType {
    /// Two sorted copies of the quad columns, each a `{s, p, o, g, rid}`
    /// child (`VarBin<Utf8>` term strings, or u32 codes under the Dictionary
    /// layout; `rid` always `u32`):
    /// - `index:posg`: the quads sorted by (p, o, s, g)
    /// - `index:ospg`: the quads sorted by (o, s, p, g)
    ///
    /// Predicate-bound patterns binary-search `index:posg`'s `p` column, a
    /// bound predicate and object prefix-search (p, o) in one probe, and
    /// object-bound patterns binary-search `index:ospg`'s `o` column; reads
    /// are served from the matched run of the copy columns. Routing engages
    /// only on children whose writer recorded them globally sorted.
    SecondaryByCopy,

    /// Sorted value columns for predicates and objects, each a `{val, rid}`
    /// child (`VarBin<Utf8>` values, or u32 codes under the Dictionary
    /// layout; `rid` always `u32`):
    /// - `index:ref-o`: the object values, sorted
    /// - `index:ref-p`: the predicate values, sorted
    ///
    /// Predicate-only and object-only patterns binary-search the matching
    /// child. Routing engages only on children whose writer recorded them
    /// globally sorted.
    SecondaryByReference,
}

/// The canonical kebab-case name (`"secondary-by-copy"`,
/// `"secondary-by-reference"`), the spelling the `clap` derive exposes.
impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            IndexType::SecondaryByCopy => "secondary-by-copy",
            IndexType::SecondaryByReference => "secondary-by-reference",
        })
    }
}

/// Accepts exactly the kebab-case names [`Display`](std::fmt::Display) emits.
impl std::str::FromStr for IndexType {
    type Err = VortexRdfError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "secondary-by-copy" => Ok(IndexType::SecondaryByCopy),
            "secondary-by-reference" => Ok(IndexType::SecondaryByReference),
            _ => Err(VortexRdfError::Deserialization(format!(
                "unknown index type {s:?}; expected \"secondary-by-copy\" or \
                 \"secondary-by-reference\""
            ))),
        }
    }
}

/// Every [`IndexType`], in declaration (preference) order.
pub(crate) const ALL_INDEX_TYPES: [IndexType; 2] =
    [IndexType::SecondaryByCopy, IndexType::SecondaryByReference];

impl IndexType {
    /// This index's persisted-child identities.
    pub(crate) const fn component_identities(self) -> &'static [ComponentIdentity] {
        match self {
            IndexType::SecondaryByCopy => &copy::IDENTITIES,
            IndexType::SecondaryByReference => &reference::IDENTITIES,
        }
    }

    /// The index whose child is named `name` (`index:posg`, …).
    pub(crate) fn of_component(name: &str) -> Option<IndexType> {
        component_named(name).map(|known| known.index)
    }

    /// The probe this index runs for `pattern`, `None` when it declines the
    /// shape.
    fn choose<'a>(
        self,
        pattern: QuadPattern<'a>,
        layout: &ResolvedLayout,
    ) -> Option<IndexProbe<'a>> {
        match self {
            IndexType::SecondaryByCopy => copy::choose(pattern, layout),
            IndexType::SecondaryByReference => reference::choose(pattern, layout),
        }
    }
}

/// The pattern roles an index resolution satisfies; residual filtering may
/// drop them.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResolvedRoles {
    Predicate,
    Object,
    /// A (p, o) prefix probe of the copy index.
    PredicateObject,
}

impl ResolvedRoles {
    /// `pattern` with the resolved roles cleared.
    pub(crate) fn clear<'a>(self, pattern: QuadPattern<'a>) -> QuadPattern<'a> {
        match self {
            ResolvedRoles::Predicate => QuadPattern {
                predicate: None,
                ..pattern
            },
            ResolvedRoles::Object => QuadPattern {
                object: None,
                ..pattern
            },
            ResolvedRoles::PredicateObject => QuadPattern {
                predicate: None,
                object: None,
                ..pattern
            },
        }
    }
}

/// What an index answers for a pattern.
///
/// `Resolved` carries `row_ids`, ascending unique base row ids; `resolves`,
/// the roles those ids already satisfy (droppable from residual filtering);
/// and `serve`, an optional plan reading the matched quads from the index's
/// own columns, a pure optimization reproducing exactly those rows. `Empty`
/// proves no row matches; `Declined` leaves the pattern to the scan path.
pub(crate) enum IndexResolution<Plan> {
    Declined,
    Empty,
    Resolved {
        row_ids: ResolvedRowIds,
        resolves: ResolvedRoles,
        serve: Option<Plan>,
    },
}

/// A resolution's row ids. `Eager` is non-empty by construction (an empty
/// result short-circuits to [`IndexResolution::Empty`]); `Lazy` rides with a
/// serve plan and may materialize empty.
pub(crate) enum ResolvedRowIds {
    Eager(Buffer<u64>),
    Lazy(LazyRowIds),
}

/// Base row ids computed on first need and shared by every clone of the view.
///
/// Two consumers racing on first need both run the computation; the source is
/// immutable, so they compute identical ids, the first store wins, and no
/// lock is held across the computation.
#[derive(Clone)]
pub(crate) struct LazyRowIds {
    cell: Arc<OnceLock<Buffer<u64>>>,
    source: LazyRowIdSource,
}

#[derive(Clone)]
enum LazyRowIdSource {
    /// The rid slice of an in-memory component's matched run.
    Component(ArrayRef),
    /// A rid scan of a file's index child.
    #[cfg(feature = "file-io")]
    IndexChild(file::FileRowIdScan),
}

impl LazyRowIds {
    /// Whether the ids have been computed.
    #[cfg(test)]
    pub(crate) fn debug_materialized(&self) -> bool {
        self.cell.get().is_some()
    }

    /// Lazy ids over an in-memory component's matched rid run.
    pub(crate) fn from_component_run(rids: ArrayRef) -> Self {
        Self {
            cell: Arc::new(OnceLock::new()),
            source: LazyRowIdSource::Component(rids),
        }
    }

    /// Lazy ids scanned from a file's index child.
    #[cfg(feature = "file-io")]
    pub(crate) fn from_file_scan(scan: file::FileRowIdScan) -> Self {
        Self {
            cell: Arc::new(OnceLock::new()),
            source: LazyRowIdSource::IndexChild(scan),
        }
    }

    /// The id count when known without computing the ids: an in-memory run's
    /// width, or a file scan's result once materialized.
    pub(crate) fn len_if_known(&self) -> Option<usize> {
        match &self.source {
            LazyRowIdSource::Component(rids) => Some(rids.len()),
            #[cfg(feature = "file-io")]
            LazyRowIdSource::IndexChild(_) => self.cell.get().map(Buffer::len),
        }
    }

    /// The ids, computed and cached on first call; runs a file child's scan.
    pub(crate) async fn materialized_async(&self) -> Result<Buffer<u64>> {
        match &self.source {
            LazyRowIdSource::Component(_) => self.materialized(),
            #[cfg(feature = "file-io")]
            LazyRowIdSource::IndexChild(scan) => {
                if let Some(ids) = self.cell.get() {
                    return Ok(ids.clone());
                }
                let ids = scan.run().await?;
                Ok(self.cell.get_or_init(|| ids).clone())
            }
        }
    }

    /// The ids, computed and cached on first call. Valid only for in-memory
    /// sources.
    pub(crate) fn materialized(&self) -> Result<Buffer<u64>> {
        if let Some(ids) = self.cell.get() {
            return Ok(ids.clone());
        }
        let ids = match &self.source {
            LazyRowIdSource::Component(rids) => sorted_row_ids(rids.clone())?,
            #[cfg(feature = "file-io")]
            LazyRowIdSource::IndexChild(_) => {
                unreachable!("an in-memory view only ever carries component-sourced pending ids")
            }
        };
        Ok(self.cell.get_or_init(|| ids).clone())
    }
}

/// The first non-`Declined` resolution of `pattern` over `indexes`, tried in
/// order, against the in-memory components.
pub(crate) fn resolve_indexes_in_memory(
    indexes: &[IndexType],
    components: &[IndexComponent],
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<InMemoryServePlan>> {
    for index in indexes {
        let Some(probe) = index.choose(pattern, layout) else {
            continue;
        };
        match resolve::resolve_in_memory(probe, components, codes)? {
            IndexResolution::Declined => continue,
            resolved => return Ok(resolved),
        }
    }
    Ok(IndexResolution::Declined)
}

/// The first non-`Declined` resolution of `pattern` over `indexes`, tried in
/// order, against a file's index children.
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_indexes_file(
    indexes: &[IndexType],
    file: &crate::store::persist::native_file::NativeStoreFile,
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<FileServePlan>> {
    for index in indexes {
        let Some(probe) = index.choose(pattern, layout) else {
            continue;
        };
        match file::resolve_file(probe, file, codes).await? {
            IndexResolution::Declined => continue,
            resolved => return Ok(resolved),
        }
    }
    Ok(IndexResolution::Declined)
}

/// The secondary indexes to embed in a store. Empty means no index children
/// are written; `vec![IndexType::SecondaryByReference]` adds the compact
/// `{val, rid}` predicate/object indexes, `vec![IndexType::SecondaryByCopy]`
/// the full sorted quad copies.
pub type Indexes = Vec<IndexType>;

/// The requested indexes without repeats, in first-seen order.
pub(crate) fn unique_indexes(indexes: &[IndexType]) -> Vec<IndexType> {
    let mut seen: Vec<IndexType> = Vec::with_capacity(indexes.len());
    for &idx in indexes {
        if !seen.contains(&idx) {
            seen.push(idx);
        }
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Term};

    /// Every declared slug resolves to its own index and component name; a
    /// slug this version does not implement resolves to nothing.
    #[test]
    fn slug_registry_covers_every_identity() {
        for index in ALL_INDEX_TYPES {
            for identity in index.component_identities() {
                let known = known_component(identity.slug).expect("declared slug is known");
                assert_eq!(known.index, index);
                assert_eq!(known.identity.name, identity.name);
            }
        }
        assert!(known_component("secondary-by-copy/spog").is_none());
        assert!(known_component("").is_none());
    }

    #[test]
    fn resolved_roles_clear() {
        let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("o"));
        let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());

        let bound = QuadPattern::new(Some(&s), Some(&p), Some(&o), Some(&g));

        let r = ResolvedRoles::Object.clear(bound);
        assert!(
            r.subject.is_some() && r.predicate.is_some() && r.object.is_none() && r.graph.is_some()
        );

        let r = ResolvedRoles::Predicate.clear(bound);
        assert!(
            r.subject.is_some() && r.predicate.is_none() && r.object.is_some() && r.graph.is_some()
        );

        let r = ResolvedRoles::PredicateObject.clear(bound);
        assert!(
            r.subject.is_some() && r.predicate.is_none() && r.object.is_none() && r.graph.is_some()
        );
    }
}
