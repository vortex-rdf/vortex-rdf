use crate::error::Result;
use crate::index::RdfDictionary;
use crate::store::layout::RdfQuadLayout;
use futures::Stream;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use std::marker::PhantomData;
use vortex_array::ArrayRef;

pub struct VortexRdfStore<Dict, Layout>
where
    Dict: RdfDictionary,
    Layout: RdfQuadLayout<Dict>,
{
    pub dictionary: Dict,
    pub quads: ArrayRef,
    _layout: PhantomData<Layout>,
}

impl<Dict, Layout> Clone for VortexRdfStore<Dict, Layout>
where
    Dict: RdfDictionary,
    Layout: RdfQuadLayout<Dict>,
{
    fn clone(&self) -> Self {
        Self {
            dictionary: self.dictionary.clone(),
            quads: self.quads.clone(),
            _layout: PhantomData,
        }
    }
}

impl<Dict, Layout> VortexRdfStore<Dict, Layout>
where
    Dict: RdfDictionary,
    Layout: RdfQuadLayout<Dict>,
{
    pub fn from_parts(dictionary: Dict, quads: ArrayRef) -> Self {
        Self {
            dictionary,
            quads,
            _layout: PhantomData,
        }
    }

    pub fn new(vortex_array: ArrayRef) -> Result<Self> {
        let dictionary = Dict::from_vortex_array(&vortex_array)?;
        let quads = Layout::extract_quads(&vortex_array)?;

        Ok(Self {
            dictionary,
            quads,
            _layout: PhantomData,
        })
    }

    pub fn empty() -> Result<Self> {
        Ok(Self {
            dictionary: Dict::new(),
            quads: Layout::empty_quads()?,
            _layout: PhantomData,
        })
    }

    #[cfg(feature = "file-io")]
    pub async fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let vortex_array = crate::io::de::load_vortex_file_path(path).await?;
        Self::new(vortex_array)
    }

    pub async fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let cursor = std::io::Cursor::new(bytes);
        let vortex_array = crate::io::de::array_from_reader(cursor)?;
        Self::new(vortex_array)
    }

    pub async fn build_vortex_index(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static,
    ) -> Result<ArrayRef> {
        Layout::build_vortex_index(quad_stream).await
    }

    pub fn get_quads_array(&self) -> Result<ArrayRef> {
        Ok(self.quads.clone())
    }

    pub fn size(&self) -> usize {
        self.quads.len()
    }

    pub fn quads(
        &self,
    ) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        Layout::quads(&self.dictionary, &self.quads)
    }

    fn with_quads(&self, quads: ArrayRef) -> Self {
        Self {
            dictionary: self.dictionary.clone(),
            quads,
            _layout: PhantomData,
        }
    }

    pub async fn match_pattern(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        let mask = Layout::find_mask(
            &self.dictionary,
            &self.quads,
            subject,
            predicate,
            object,
            graph,
        )?;

        if let Some(m) = mask {
            let filtered_quads = crate::store::layout::filter_with_bool_mask(&self.quads, m)?;
            Ok(self.with_quads(filtered_quads))
        } else {
            Ok(self.clone())
        }
    }

    pub async fn add_quad(&self, quad: Quad) -> Result<Self> {
        let mut dictionary = self.dictionary.clone();
        let quads = Layout::add_quad(&mut dictionary, &self.quads, quad)?;

        Ok(Self {
            dictionary,
            quads,
            _layout: PhantomData,
        })
    }

    pub async fn delete_quad(&self, quad: &Quad) -> Result<Self> {
        let quads = Layout::delete_quad(&self.dictionary, &self.quads, quad)?;
        Ok(self.with_quads(quads))
    }
}

impl<Dict, Layout> crate::store::QuadStore for VortexRdfStore<Dict, Layout>
where
    Dict: RdfDictionary,
    Layout: RdfQuadLayout<Dict>,
{
    fn quads(
        &self,
    ) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        self.quads()
    }
}