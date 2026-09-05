//! The resident forms an adopted store's columns take — the choice
//! `from_bytes` and `from_parts` make for the term dictionary and for the
//! `u32` code columns (see `docs/memory.md` §1).

use crate::error::{Result, VortexRdfError};
use crate::store::DictForm;

/// How an adopted store holds its `u32` code columns — the base's and its
/// index components'. A built store always holds them canonical; this choice
/// applies where the columns arrive already encoded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CodeForm {
    /// The writer's encodings, as refcounted views into the bytes: a wide
    /// code read decodes a column into a live canonical form, shared while
    /// some holder keeps it and freed with the last.
    #[default]
    AsWritten,
    /// The base's columns decoded once into canonical primitives, so every
    /// code read is a slice of them; a component's live canonical form is
    /// pinned for the store's lifetime once a served read fills it.
    Canonical,
}

impl CodeForm {
    /// The canonical kebab-case name, shared by every frontend.
    pub fn name(self) -> &'static str {
        match self {
            CodeForm::AsWritten => "as-written",
            CodeForm::Canonical => "canonical",
        }
    }
}

impl std::fmt::Display for CodeForm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for CodeForm {
    type Err = VortexRdfError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "as-written" => Ok(CodeForm::AsWritten),
            "canonical" => Ok(CodeForm::Canonical),
            other => Err(VortexRdfError::InvalidOperation(format!(
                "unknown code form {other:?}; expected \"as-written\" or \"canonical\""
            ))),
        }
    }
}

/// The resident forms of an adopted store: its term dictionary's and its
/// code columns'. The default is the lean load — everything as written.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResidentForm {
    pub dict: DictForm,
    pub codes: CodeForm,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_form_names_round_trip() {
        for form in [CodeForm::AsWritten, CodeForm::Canonical] {
            assert_eq!(form.name().parse::<CodeForm>().unwrap(), form);
            assert_eq!(form.to_string(), form.name());
        }
        let err = "compressed".parse::<CodeForm>().unwrap_err().to_string();
        assert!(
            err.contains("as-written") && err.contains("canonical"),
            "{err}"
        );
        assert_eq!(
            ResidentForm::default(),
            ResidentForm {
                dict: DictForm::AsWritten,
                codes: CodeForm::AsWritten
            }
        );
    }
}
