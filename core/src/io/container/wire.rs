//! The persisted component inventory and its wire codec.
//!
//! The inventory rides in the root layout's metadata: name, role,
//! implementation slug, version, the required flag, and the component's
//! column shape in a small field-kind vocabulary (no serialized DTypes).
//! Unknown *optional* components are readable-around by construction;
//! unknown *required* components must fail the open — a reader that skipped
//! a required change set would silently resurrect deleted rows.

use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vortex_array::dtype::{DType, Nullability, PType, StructFields};
use vortex_error::{VortexResult, vortex_bail, vortex_ensure_eq};

use super::QUAD_SOURCE_NAME;

const STORE_METADATA_VERSION: u32 = 1;

/// Persisted role of an auxiliary child. `ChangeSet` is reserved for
/// immutable delta components (write those `required: true` — see module
/// doc); `Other` keeps the vocabulary open without a wire break.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum StoreComponentRole {
    Dictionary,
    Index,
    ChangeSet,
    Other,
}

/// The column-type vocabulary components may use on the wire. Every
/// component is a non-nullable struct of these leaves; extending the
/// vocabulary is backward-compatible (old readers fail to parse only files
/// that actually use the new kind).
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
    use super::{DType, Deserialize, Deserializer, Serialize, Serializer, WireField};

    pub(super) fn serialize<S: Serializer>(
        dtype: &DType,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        super::dtype_to_wire_fields(dtype)
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DType, D::Error> {
        Vec::<WireField>::deserialize(deserializer)
            .map(|fields| super::wire_fields_to_dtype(&fields))
    }
}

/// Descriptor of one auxiliary child, as persisted in the root metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoreComponentDescriptor {
    pub(crate) name: String,
    pub(crate) role: StoreComponentRole,
    /// Implementation slug (e.g. `sorted-terms-fsst-v1`) — how a reader that
    /// knows the component interprets its columns.
    pub(crate) implementation: String,
    pub(crate) version: u32,
    /// Readers must reject the file when they cannot interpret a required
    /// component; unknown optional components are skipped.
    pub(crate) required: bool,
    /// Whether the component's sort-key columns are GLOBALLY sorted (not
    /// merely per-chunk). Provenance, recorded by the writer that knows how
    /// the component was built — a reader lifting the component into memory
    /// may only binary-search it when this is set. Stamping per-chunk-sorted
    /// data as sorted corrupts query results, so absent/false is the safe
    /// default.
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

/// Validate a component inventory: every descriptor individually
/// ([`StoreComponentDescriptor::validate`]) plus name uniqueness across the
/// set. The single owner of inventory validation, run once per container
/// entry path — [`decode_store_metadata`] on read,
/// `RdfStoreWriteStrategy::with_components` on write.
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

#[derive(Serialize, Deserialize)]
struct WireMetadata {
    version: u32,
    /// Whether the quad rows are in GLOBAL `(s, p, o, g)` order — the
    /// writer's provenance (a sorted builder or rebuild produced them). A
    /// reader materializing the quads may only restore the sorted stamp when
    /// this is set, and that stamp licenses binary search on every role of a
    /// bound prefix, not the subject alone; the file's own statistics do not
    /// record sortedness, and a false claim corrupts matches.
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
