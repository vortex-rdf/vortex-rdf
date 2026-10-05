//! The persisted component inventory and its wire codec: the root layout's
//! JSON metadata (`version`, `quads_sorted`, `components`), each component's
//! descriptor, and the field-kind vocabulary a component's column shape is
//! written in.

use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vortex_array::dtype::{DType, Nullability, PType, StructFields};
use vortex_error::{VortexResult, vortex_bail, vortex_ensure_eq};

use super::QUAD_SOURCE_NAME;

const STORE_METADATA_VERSION: u32 = 1;

/// Persisted role of an auxiliary child. `ChangeSet` is reserved for delta
/// components (written `required: true`); `Other` keeps the vocabulary open
/// without a wire break.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum StoreComponentRole {
    Dictionary,
    Index,
    ChangeSet,
    Other,
}

/// The wire column-type vocabulary: every component is a non-nullable struct
/// of these leaves. Adding a kind is backward-compatible (old readers reject
/// only files that use it).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum WireFieldKind {
    U32,
    U64,
    Utf8,
}

impl WireFieldKind {
    fn to_dtype(self) -> DType {
        match self {
            Self::U32 => DType::Primitive(PType::U32, Nullability::NonNullable),
            Self::U64 => DType::Primitive(PType::U64, Nullability::NonNullable),
            Self::Utf8 => DType::Utf8(Nullability::NonNullable),
        }
    }

    fn from_dtype(dtype: &DType) -> Option<Self> {
        match dtype {
            DType::Primitive(PType::U32, Nullability::NonNullable) => Some(Self::U32),
            DType::Primitive(PType::U64, Nullability::NonNullable) => Some(Self::U64),
            DType::Utf8(Nullability::NonNullable) => Some(Self::Utf8),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct WireField {
    name: String,
    kind: WireFieldKind,
}

fn wire_fields_to_dtype(fields: &[WireField]) -> DType {
    DType::Struct(
        StructFields::new(
            fields
                .iter()
                .map(|f| f.name.as_str().into())
                .collect::<Vec<Arc<str>>>()
                .into(),
            fields.iter().map(|f| f.kind.to_dtype()).collect(),
        ),
        Nullability::NonNullable,
    )
}

fn dtype_to_wire_fields(dtype: &DType) -> VortexResult<Vec<WireField>> {
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        vortex_bail!("store component dtype must be a non-nullable struct, got {dtype}");
    };
    fields
        .names()
        .iter()
        .zip(fields.fields())
        .map(|(name, field)| {
            let kind = WireFieldKind::from_dtype(&field).ok_or_else(|| {
                vortex_error::vortex_err!(
                    "store component field {name} has a dtype outside the wire vocabulary: {field}"
                )
            })?;
            Ok(WireField {
                name: name.to_string(),
                kind,
            })
        })
        .collect()
}

/// A component dtype on the wire: its `fields` list in the kind vocabulary.
mod wire_fields {
    use super::*;

    pub(super) fn serialize<S: Serializer>(
        dtype: &DType,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        dtype_to_wire_fields(dtype)
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DType, D::Error> {
        Vec::<WireField>::deserialize(deserializer).map(|fields| wire_fields_to_dtype(&fields))
    }
}

/// Descriptor of one auxiliary child, as persisted in the root metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoreComponentDescriptor {
    pub(crate) name: String,
    pub(crate) role: StoreComponentRole,
    /// Implementation slug (`sorted-terms-fsst-v1`, `secondary-by-copy/posg`,
    /// …): how a reader interprets the columns.
    pub(crate) implementation: String,
    pub(crate) version: u32,
    /// A reader that cannot interpret a required component rejects the file;
    /// an unknown optional component is skipped.
    pub(crate) required: bool,
    /// Whether the sort-key columns are GLOBALLY sorted (not per chunk): the
    /// writer's provenance, and a reader's licence to binary-search the
    /// component. Absent on the wire means `false`.
    #[serde(default)]
    pub(crate) sorted: bool,
    /// The column shape, written as `fields` in the kind vocabulary.
    #[serde(rename = "fields", with = "wire_fields")]
    pub(crate) dtype: DType,
}

impl StoreComponentDescriptor {
    pub(crate) fn validate(&self) -> VortexResult<()> {
        if self.name.is_empty() {
            vortex_bail!("store component name must not be empty");
        }
        if self.name == QUAD_SOURCE_NAME {
            vortex_bail!("{QUAD_SOURCE_NAME} is reserved for the transparent root child");
        }
        if self.implementation.is_empty() {
            vortex_bail!("store component implementation must not be empty");
        }
        if self.version == 0 {
            vortex_bail!("store component version must be positive");
        }
        dtype_to_wire_fields(&self.dtype)?;
        Ok(())
    }
}

/// Validate an inventory: every descriptor, plus name uniqueness across the
/// set; run once on decode and once on write.
pub(super) fn validate_components<'a>(
    components: impl IntoIterator<Item = &'a StoreComponentDescriptor>,
) -> VortexResult<()> {
    let mut names = std::collections::BTreeSet::new();
    for descriptor in components {
        descriptor.validate()?;
        if !names.insert(descriptor.name.as_str()) {
            vortex_bail!("duplicate store component name: {}", descriptor.name);
        }
    }
    Ok(())
}

/// The root metadata. `quads_sorted` records that the quad rows are in
/// GLOBAL `(s, p, o, g)` order: a reader may restore the subject sorted
/// stamp only when it is set, and the stamp licenses binary search on every
/// role of a bound prefix. Absent on the wire means `false`.
#[derive(Serialize, Deserialize)]
struct WireMetadata {
    version: u32,
    #[serde(default)]
    quads_sorted: bool,
    components: Vec<StoreComponentDescriptor>,
}

pub(super) fn encode_store_metadata(
    quads_sorted: bool,
    components: &[StoreComponentDescriptor],
) -> VortexResult<Vec<u8>> {
    let wire = WireMetadata {
        version: STORE_METADATA_VERSION,
        quads_sorted,
        components: components.to_vec(),
    };
    serde_json::to_vec(&wire).map_err(|e| vortex_error::vortex_err!("{e}"))
}

/// Empty bytes decode as an unsorted store with no components.
pub(super) fn decode_store_metadata(
    bytes: &[u8],
) -> VortexResult<(bool, Vec<StoreComponentDescriptor>)> {
    if bytes.is_empty() {
        return Ok((false, Vec::new()));
    }
    let wire: WireMetadata =
        serde_json::from_slice(bytes).map_err(|e| vortex_error::vortex_err!("{e}"))?;
    vortex_ensure_eq!(
        wire.version,
        STORE_METADATA_VERSION,
        "unsupported vortex-rdf store metadata version"
    );
    validate_components(&wire.components)?;
    Ok((wire.quads_sorted, wire.components))
}
