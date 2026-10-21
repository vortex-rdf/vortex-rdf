//! Reading a view out: counts, rows, code columns, decoded quads, chunk
//! streams and the vortex data sources over them, and the metadata that
//! describes a view before it is read.

pub(crate) mod chunks;
pub(crate) mod count;
pub(crate) mod data_source;
pub(crate) mod metadata;
pub(crate) mod quads;
pub(crate) mod rows;

use std::future::Future;

use futures::stream::{self, BoxStream, Stream, StreamExt};

use crate::error::{Result, VortexRdfError};

/// A stream that builds its source on the first poll, so the view's read runs
/// then and not when the stream is requested; a failed build yields `on_err`
/// of its error as the stream's one item.
fn deferred<'a, S, I>(
    build: impl Future<Output = Result<S>> + Send + 'a,
    on_err: fn(VortexRdfError) -> I,
) -> BoxStream<'a, I>
where
    S: Stream<Item = I> + Send + 'a,
    I: Send + 'a,
{
    stream::once(build)
        .flat_map(move |built| match built {
            Ok(items) => items.left_stream(),
            Err(e) => stream::iter([on_err(e)]).right_stream(),
        })
        .boxed()
}
