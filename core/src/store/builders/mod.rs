//! The builders' hub: the [`VortexArrayBuilder`] contract and its products
//! ([`BuiltArray`], [`BuiltStream`]), primary chunk assembly and emission, and
//! the globally sorted index emission ([`build_components`]). Every build
//! sorts globally by (s, p, o, g) and its components are globally sorted,
//! hence `sorted: true`; `sorted_stream` is the pipeline wherever a filesystem
//! exists, `sorted_in_memory` on wasm32-unknown-unknown.

use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
use crate::store::array::stamp_is_sorted;
use crate::store::indexes::{IndexComponent, IndexType, Indexes, copy, reference, unique_indexes};
use crate::store::layouts::dictionary::{QuadCodes, TermDictionary};
use crate::store::layouts::{LayoutStrategy, dictionary};
use futures::{Stream, StreamExt as _, stream};
use std::future::Future;
use std::sync::Arc;
use vortex_array::arrays::StructArray;
use vortex_array::dtype::DType;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};

/// Number of quads per StructArray chunk in streaming/chunked builders.
pub(crate) const DEFAULT_CHUNK_ROWS: usize = 100_000;

/// A stream of StructArray chunks for the Vortex file writer; items are
/// `VortexResult` because the writer polls the stream directly.
pub type ChunkStream = stream::BoxStream<'static, vortex_error::VortexResult<ArrayRef>>;

/// A builder error as a `VortexError`, for a [`ChunkStream`].
fn into_vortex_error(e: VortexRdfError) -> vortex_error::VortexError {
    match e {
        VortexRdfError::Vortex(v) => v,
        other => vortex_error::vortex_err!("{}", other),
    }
}

/// A built dataset: the quad array, its index components and, under the
/// Dictionary layout, the term dictionary the codes address. Cloning is
/// shallow.
#[derive(Clone)]
pub struct BuiltArray {
    /// The quad rows as one struct array in the layout's schema.
    pub array: ArrayRef,
    /// The requested indexes' children; empty when none were requested.
    pub(crate) components: Vec<IndexComponent>,
    pub(crate) dict: Option<Arc<TermDictionary>>,
}

/// The streaming counterpart of [`BuiltArray`]: the schema dtype, the lazy
/// stream of primary chunks, the index children as writable components, and
/// the dictionary the serializer writes as the `dictionary` child.
pub struct BuiltStream {
    /// The schema dtype shared by every chunk.
    pub dtype: DType,
    /// The lazy stream of primary-only quad chunks.
    pub chunks: ChunkStream,
    /// The index children riding beside the rows as writable components.
    pub(crate) components: Vec<crate::io::container::NativeComponentWrite>,
    /// Whether the chunks are in global `(s, p, o, g)` order; written as the
    /// root's `quads_sorted`.
    #[cfg_attr(
        not(any(feature = "file-io", target_arch = "wasm32")),
        allow(dead_code)
    )]
    pub(crate) quads_sorted: bool,
    /// The Dictionary layout's terms, written as the `dictionary` child.
    #[cfg_attr(
        not(any(feature = "file-io", target_arch = "wasm32")),
        allow(dead_code)
    )]
    pub(crate) dict: Option<Arc<TermDictionary>>,
}

impl BuiltStream {
    /// A stream of globally sorted chunks with `components` and `dict`
    /// beside it.
    pub(crate) fn sorted(
        dtype: DType,
        chunks: ChunkStream,
        components: Vec<crate::io::container::NativeComponentWrite>,
        dict: Option<Arc<TermDictionary>>,
    ) -> Self {
        Self {
            dtype,
            chunks,
            components,
            quads_sorted: true,
            dict,
        }
    }
}

/// The chunk emission every builder shares: `next` yields the next chunk of
/// at most `chunk_size` rows off `source`, `None` once exhausted; `empty`
/// supplies the schema-carrying chunk of an empty dataset. The first chunk
/// is built before returning so the dtype is known up front; the rest are
/// built as polled.
pub(crate) fn chunk_stream<S: Send + 'static>(
    mut source: S,
    chunk_size: usize,
    mut next: impl FnMut(&mut S, usize) -> Result<Option<ArrayRef>> + Send + 'static,
    empty: impl FnOnce() -> Result<ArrayRef>,
) -> Result<(DType, ChunkStream)> {
    let first = match next(&mut source, chunk_size)? {
        Some(chunk) => chunk,
        None => empty()?,
    };
    let dtype = first.dtype().clone();
    let rest = stream::unfold((source, next), move |(mut source, mut next)| async move {
        match next(&mut source, chunk_size) {
            Ok(None) => None,
            Ok(Some(chunk)) => Some((Ok(chunk), (source, next))),
            Err(e) => Some((Err(into_vortex_error(e)), (source, next))),
        }
    });
    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}

pub(crate) mod sorted_in_memory;
// No filesystem to spill to on wasm32-unknown-unknown.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod sorted_stream;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod spill;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod stream_indexes;

pub use sorted_in_memory::SortedInMemoryBuilder;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use sorted_stream::SortedStreamBuilder;

/// A build pipeline from a quad stream to store parts, sorting globally by
/// (s, p, o, g): [`build_vortex_array`](Self::build_vortex_array)
/// materializes the dataset, [`build_vortex_stream`](Self::build_vortex_stream)
/// emits it lazily for the file writer.
pub trait VortexArrayBuilder {
    /// The complete dataset as one (possibly chunked) array with its
    /// components and dictionary.
    fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> impl Future<Output = Result<BuiltArray>> + Send;

    /// The schema dtype and a lazily evaluated chunk stream for the file
    /// writer; O(chunk) memory for the column arrays.
    fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> impl Future<Output = Result<BuiltStream>> + Send;
}

/// One StructArray chunk of primary columns for `layout` (the Dictionary
/// layout only as the empty chunk; its rows need the dictionary pipeline).
/// `s_sorted` stamps `IsSorted` on the `s` column and must be `true` only
/// when `quads` is globally sorted.
pub(crate) fn build_struct_array(
    quads: &[RawQuad],
    layout: LayoutStrategy,
    s_sorted: bool,
) -> Result<ArrayRef> {
    if layout == LayoutStrategy::Dictionary && quads.is_empty() {
        return dictionary::build_code_chunk(&QuadCodes::default(), 0..0, s_sorted);
    }
    let field_names = layout.field_names();
    let field_arrays = layout.build_columns(quads)?;

    if s_sorted {
        // `s` is the first column of every layout.
        stamp_is_sorted(&field_arrays[0]);
    }

    Ok(StructArray::try_new(
        field_names.into(),
        field_arrays,
        quads.len(),
        Validity::NonNullable,
    )?
    .into_array())
}

/// Every requested index's columns, sorted once over the complete in-memory
/// dataset.
struct GlobalIndexes {
    by_copy: Option<copy::GlobalCopyArrays>,
    by_reference: Option<reference::GlobalReferenceArrays>,
}

impl GlobalIndexes {
    /// The requested families over the dataset in final row order; `copy`
    /// and `reference` run only when their family is requested.
    fn build(
        indexes: &[IndexType],
        copy: impl FnOnce() -> copy::GlobalCopyArrays,
        reference: impl FnOnce() -> reference::GlobalReferenceArrays,
    ) -> Self {
        let unique = unique_indexes(indexes);
        Self {
            by_copy: unique.contains(&IndexType::SecondaryByCopy).then(copy),
            by_reference: unique
                .contains(&IndexType::SecondaryByReference)
                .then(reference),
        }
    }

    /// Every built index's children in index declaration order, each
    /// globally sorted.
    fn into_components(self) -> Result<Vec<IndexComponent>> {
        let mut components = Vec::new();
        if let Some(sbc) = self.by_copy {
            components.extend(sbc.into_components()?);
        }
        if let Some(sbr) = self.by_reference {
            components.extend(sbr.into_components()?);
        }
        Ok(components)
    }
}

/// The requested indexes' children over a complete in-memory dataset in
/// final row order.
pub(crate) fn build_components(
    indexes: &[IndexType],
    quads: &[RawQuad],
) -> Result<Vec<IndexComponent>> {
    GlobalIndexes::build(
        indexes,
        || copy::GlobalCopyArrays::from_quads(quads),
        || reference::GlobalReferenceArrays::from_quads(quads),
    )
    .into_components()
}

/// [`build_components`] over the dataset's u32 codes; code order is term
/// order, so the children stay binary-searchable.
pub(crate) fn build_components_from_codes(
    indexes: &[IndexType],
    codes: &QuadCodes,
) -> Result<Vec<IndexComponent>> {
    GlobalIndexes::build(
        indexes,
        || copy::GlobalCopyArrays::from_codes(codes),
        || reference::GlobalReferenceArrays::from_codes(codes),
    )
    .into_components()
}

/// A store's parts rebuilt from raw quads under `strategy`: the primary
/// rows, the requested indexes' components and, under the Dictionary layout,
/// a fresh term dictionary. `sorted` must be `true` only when `raws` is
/// SPOG-sorted; the components are globally sorted whatever the row order.
pub(crate) fn build_parts_from_raws(
    raws: &[RawQuad],
    strategy: LayoutStrategy,
    indexes: &[IndexType],
    sorted: bool,
) -> Result<BuiltArray> {
    match strategy {
        LayoutStrategy::Dictionary => {
            let (dict, code_map) = TermDictionary::from_quads_with_map(raws)?;
            let codes = dictionary::encode_quads(raws, &code_map)?;
            Ok(BuiltArray {
                array: dictionary::build_code_chunk(&codes, 0..raws.len(), sorted)?,
                components: build_components_from_codes(indexes, &codes)?,
                dict: Some(Arc::new(dict)),
            })
        }
        strategy => Ok(BuiltArray {
            array: build_struct_array(raws, strategy, sorted)?,
            components: build_components(indexes, raws)?,
            dict: None,
        }),
    }
}
