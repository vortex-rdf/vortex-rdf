//! The streaming twin of [`split_built_row_space`]: tee a builder's welded
//! row-space chunk stream into the primary quad stream plus one
//! channel-backed native component per index family.
//!
//! Permanent bridge, not scaffolding: a builder that slices one global index
//! emission into its chunks emits row-space chunks by design, so this is its
//! component adapter. It lives with the indexes because everything it splits
//! by — specs, column rosters, the strip — is index vocabulary; the
//! serializer just drives it.
//!
//! [`split_built_row_space`]: super::split_built_row_space

use std::sync::Arc;

use futures::StreamExt as _;
use vortex_array::arrays::StructArray;
use vortex_array::dtype::DType;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_array::{ArrayRef, VortexSessionExecute as _};

use crate::error::{Result, VortexRdfError};
use crate::io::container::sources::NativeComponentSource;
use crate::io::container::{NativeComponentWrite, default_child_strategy};
use crate::session::VORTEX_SESSION;
use crate::store::builders::{ChunkStream, into_vortex_error};

use super::components::child_struct;
use super::{
    ComponentRole, IndexComponentSpec, index_component_specs, primary_dtype, strip_index_columns,
};

/// Rebuild a role's columns out of a row-space chunk as a standalone struct
/// under the child's own column names (e.g. `_idx_posg_s → s`). The arrays
/// are shared, so per-column statistics (the `IsSorted` lead stamps) travel
/// with them.
fn project_chunk(chunk: &ArrayRef, role: &ComponentRole) -> Result<ArrayRef> {
    use vortex_array::IntoArray as _;
    use vortex_array::arrays::struct_::StructArrayExt as _;
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = chunk
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let arrays: Vec<ArrayRef> = role
        .source_columns
        .iter()
        .map(|n| {
            struct_arr
                .unmasked_field_by_name(n)
                .cloned()
                .map_err(VortexRdfError::Vortex)
        })
        .collect::<Result<_>>()?;
    let len = struct_arr.len();
    child_struct(role.child_columns, arrays, len).map(|arr| arr.into_array())
}

/// A component source fed chunk-by-chunk by the quad stream's own poll loop
/// (the row-space splitter's tee). Channel-backed: see
/// [`NativeComponentSource::channel_backed`] for the concurrency contract.
struct ChannelComponentSource {
    dtype: DType,
    receiver: std::sync::Mutex<
        Option<futures::channel::mpsc::Receiver<vortex_error::VortexResult<ArrayRef>>>,
    >,
}

impl NativeComponentSource for ChannelComponentSource {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn open(&self) -> vortex_error::VortexResult<vortex_array::stream::SendableArrayStream> {
        let receiver = self
            .receiver
            .lock()
            .expect("channel source lock")
            .take()
            .ok_or_else(|| {
                vortex_error::vortex_err!("a channel-backed component source replays only once")
            })?;
        Ok(vortex_array::stream::ArrayStreamExt::boxed(
            ArrayStreamAdapter::new(self.dtype.clone(), receiver),
        ))
    }

    fn channel_backed(&self) -> bool {
        true
    }
}

/// A row-space chunk stream split for the native container: the primary quad
/// columns stream to the transparent quad-source child while each index
/// family's columns tee into a channel-backed component.
pub(crate) struct SplitRowSpace {
    pub(crate) quad_dtype: DType,
    pub(crate) quad_chunks: ChunkStream,
    pub(crate) components: Vec<NativeComponentWrite>,
}

pub(crate) fn split_row_space(
    dtype: DType,
    chunks: ChunkStream,
    components_sorted: bool,
) -> Result<SplitRowSpace> {
    let specs: Vec<IndexComponentSpec> = index_component_specs(&dtype, components_sorted)?;
    let quad_dtype = primary_dtype(&dtype);
    if specs.is_empty() {
        return Ok(SplitRowSpace {
            quad_dtype,
            quad_chunks: chunks,
            components: Vec::new(),
        });
    }

    let mut senders = Vec::with_capacity(specs.len());
    let mut components = Vec::with_capacity(specs.len());
    for spec in &specs {
        let (sender, receiver) = futures::channel::mpsc::channel(1);
        senders.push(sender);
        components.push(
            NativeComponentWrite::new(
                spec.descriptor.clone(),
                Arc::new(ChannelComponentSource {
                    dtype: spec.descriptor.dtype.clone(),
                    receiver: std::sync::Mutex::new(Some(receiver)),
                }),
                default_child_strategy(),
            )
            .map_err(VortexRdfError::Vortex)?,
        );
    }

    let quad_chunks: ChunkStream = Box::pin(futures::stream::unfold(
        (chunks, senders, specs),
        |(mut chunks, mut senders, specs)| async move {
            use futures::SinkExt as _;
            let chunk = match chunks.next().await? {
                Ok(chunk) => chunk,
                Err(e) => return Some((Err(e), (chunks, senders, specs))),
            };
            for (spec, sender) in specs.iter().zip(senders.iter_mut()) {
                let child = match project_chunk(&chunk, spec.role) {
                    Ok(child) => child,
                    Err(e) => {
                        return Some((Err(into_vortex_error(e)), (chunks, senders, specs)));
                    }
                };
                if sender.send(Ok(child)).await.is_err() {
                    return Some((
                        Err(vortex_error::vortex_err!(
                            "index component writer hung up mid-stream"
                        )),
                        (chunks, senders, specs),
                    ));
                }
            }
            let primary = strip_index_columns(chunk).map_err(into_vortex_error);
            Some((primary, (chunks, senders, specs)))
        },
    ));

    Ok(SplitRowSpace {
        quad_dtype,
        quad_chunks,
        components,
    })
}
