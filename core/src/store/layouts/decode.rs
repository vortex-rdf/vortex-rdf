//! The representations a chunk's rows decode into ([`ChunkDecode`]): owned
//! oxrdf quads, [`SharedQuad`]s whose terms are shared N-Triples strings,
//! one [`NQuadLines`] of the chunk's rows written out, or [`RawQuad`]s of
//! owned term strings. Each reads the rows from wherever the layout keeps its
//! terms ([`LayoutSource`]), the dispatch written once here: a string
//! layout's columns ([`ChunkDecode::from_strings`]), or a Dictionary layout's
//! codes against its resident or file-backed dictionary
//! ([`DecodeRows`](super::dictionary::DecodeRows)).

#[cfg(feature = "file-io")]
use futures::FutureExt as _;
#[cfg(feature = "file-io")]
use futures::future::{self, BoxFuture};
use oxrdf::Quad;
use vortex_array::ArrayRef;

use super::ResolvedLayout;
#[cfg(feature = "file-io")]
use super::dictionary::FileBackedDict;
use super::dictionary::{self, DecodeRows, DictAccess, TermDictionary};
use super::strings::StringLayout;
use crate::common::quad::{NQuadLines, SharedQuad};
use crate::error::Result;
use crate::store::RawQuad;

/// Where a layout keeps its rows' terms.
pub(crate) enum LayoutSource<'a> {
    /// A string layout's own columns.
    Strings(StringLayout),
    /// A Dictionary layout's codes, against its resident dictionary.
    Resident(&'a TermDictionary),
    /// A Dictionary layout's codes, against its file-backed dictionary, whose
    /// terms are read through its windows.
    #[cfg(feature = "file-io")]
    FileBacked(&'a FileBackedDict),
}

impl ResolvedLayout {
    /// Where this layout keeps its rows' terms.
    pub(crate) fn source(&self) -> LayoutSource<'_> {
        match self {
            ResolvedLayout::Default => LayoutSource::Strings(StringLayout::Default),
            ResolvedLayout::TypedObject => LayoutSource::Strings(StringLayout::TypedObject),
            ResolvedLayout::Dictionary(DictAccess::Resident(dict)) => LayoutSource::Resident(dict),
            #[cfg(feature = "file-io")]
            ResolvedLayout::Dictionary(DictAccess::FileBacked(fb)) => LayoutSource::FileBacked(fb),
        }
    }

    /// An array of this layout's rows decoded back into [`RawQuad`]s, each
    /// term in its N-Triples string form, without an oxrdf parse round-trip.
    ///
    /// The inverse of the build-time column encoding, for the operations that
    /// rebuild a store from its quads (compaction, and reads that must merge a
    /// string tail into a Dictionary-encoded base). A file-backed dictionary
    /// is an error here; see [`raw_quads_async`](Self::raw_quads_async).
    pub(crate) fn raw_quads(&self, rows: &ArrayRef) -> Result<Vec<RawQuad>> {
        RawQuad::decode(self, rows).into_iter().collect()
    }

    /// [`raw_quads`](Self::raw_quads) with the file-backed Dictionary case
    /// handled: the whole dictionary is lifted resident transiently, then the
    /// sync decode runs unchanged. Its one caller, `live_raw_quads`, serves
    /// whole-base reads only (compaction and serialization), which touch most
    /// of the dictionary anyway; every chunk a read streams decodes through
    /// [`ChunkDecode::decode_async`] instead.
    pub(crate) async fn raw_quads_async(&self, rows: &ArrayRef) -> Result<Vec<RawQuad>> {
        #[cfg(feature = "file-io")]
        if let ResolvedLayout::Dictionary(access) = self
            && access.is_file_backed()
        {
            let dict = access.ensure_resident().await?;
            let resident = ResolvedLayout::Dictionary(DictAccess::Resident(dict));
            return resident.raw_quads(rows);
        }
        self.raw_quads(rows)
    }
}

/// A representation a chunk's rows decode into. Each pipeline over chunks
/// (the serve plans, the reads by id, the scan streams, the tail) is written
/// once against this trait for every representation, and the dispatch over
/// where the layout keeps its terms is written once, in the provided
/// methods: a representation supplies its string-layout read
/// ([`from_strings`](Self::from_strings)) and its decode of codes
/// ([`DecodeRows`]).
///
/// A chunk-level failure (execution, a required column missing or of the
/// wrong type) is the single `Err` element; a row whose terms fail to parse
/// is an `Err` at that row's position, the other rows decoding normally.
pub(crate) trait ChunkDecode: DecodeRows + Sized + Send + 'static {
    /// A string layout's `chunk` rows.
    fn from_strings(layout: StringLayout, chunk: &ArrayRef) -> Vec<Result<Self>>;

    /// `chunk`'s rows through `layout`, a resident dictionary's codes
    /// decoded against it; a file-backed dictionary is a chunk-level error
    /// (its chunks decode through [`decode_async`](Self::decode_async)).
    fn decode(layout: &ResolvedLayout, chunk: &ArrayRef) -> Vec<Result<Self>> {
        match layout.source() {
            LayoutSource::Strings(strings) => Self::from_strings(strings, chunk),
            LayoutSource::Resident(dict) => dictionary::decode_resident(chunk, dict),
            #[cfg(feature = "file-io")]
            LayoutSource::FileBacked(_) => {
                vec![Err(crate::error::VortexRdfError::Deserialization(
                    "a file-backed dictionary decodes chunks through the async read path"
                        .to_string(),
                ))]
            }
        }
    }

    /// [`decode`](Self::decode), a file-backed dictionary's codes decoded
    /// against it: the chunk's code columns are read once and decoded in
    /// place through the windows their codes fall in, or, when those outgrow
    /// the window cache or their reads would wait, against their distinct
    /// codes' terms, read with one read of each window holding them. Every
    /// other layout, and a resident dictionary, decodes without suspending.
    #[cfg(feature = "file-io")]
    fn decode_async<'a>(
        layout: &'a ResolvedLayout,
        chunk: &'a ArrayRef,
    ) -> BoxFuture<'a, Vec<Result<Self>>> {
        match layout.source() {
            LayoutSource::FileBacked(fb) => dictionary::decode_file_backed(fb, chunk).boxed(),
            _ => future::ready(Self::decode(layout, chunk)).boxed(),
        }
    }

    /// [`decode_async`](Self::decode_async) where it finishes without
    /// waiting, for a caller that cannot await (a scan's split task): every
    /// layout but a file-backed dictionary's, and that one while its window
    /// reads complete when first polled, as over a mapped file. `None` when
    /// a read would wait, and without reading once the dictionary's reads
    /// have had to: the chunk is then `decode_async`'s.
    #[cfg(feature = "file-io")]
    fn decode_now(layout: &ResolvedLayout, chunk: &ArrayRef) -> Option<Vec<Result<Self>>> {
        match layout.source() {
            LayoutSource::FileBacked(fb) if fb.reads_wait() => None,
            LayoutSource::FileBacked(fb) => {
                dictionary::decode_file_backed(fb, chunk).now_or_never()
            }
            _ => Some(Self::decode(layout, chunk)),
        }
    }
}

/// A chunk-level result as the per-row convention has it: the rows, or the
/// one error.
fn rows_or_error<T>(rows: Result<Vec<T>>) -> Vec<Result<T>> {
    match rows {
        Ok(rows) => rows.into_iter().map(Ok).collect(),
        Err(e) => vec![Err(e)],
    }
}

impl ChunkDecode for Quad {
    fn from_strings(layout: StringLayout, chunk: &ArrayRef) -> Vec<Result<Self>> {
        layout.quads(chunk)
    }
}

impl ChunkDecode for SharedQuad {
    fn from_strings(layout: StringLayout, chunk: &ArrayRef) -> Vec<Result<Self>> {
        rows_or_error(layout.shared_quads(chunk))
    }
}

impl ChunkDecode for NQuadLines {
    fn from_strings(layout: StringLayout, chunk: &ArrayRef) -> Vec<Result<Self>> {
        vec![layout.nquad_lines(chunk)]
    }
}

impl ChunkDecode for RawQuad {
    fn from_strings(layout: StringLayout, chunk: &ArrayRef) -> Vec<Result<Self>> {
        rows_or_error(layout.raw_quads(chunk))
    }
}
