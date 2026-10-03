//! The RDF and XSD vocabulary IRIs read off term spellings; the two datatype
//! IRIs come from `oxrdf`'s own vocabulary.

/// The XSD namespace, the prefix of every built-in datatype IRI.
pub(crate) const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// `xsd:string`, the datatype of a plain literal; dropped from the stored
/// spelling and from the TypedObject datatype column.
pub(crate) const XSD_STRING: &str = oxrdf::vocab::xsd::STRING.as_str();

/// `rdf:langString`, the datatype of a language-tagged literal.
pub(crate) const RDF_LANG_STRING: &str = oxrdf::vocab::rdf::LANG_STRING.as_str();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vocabulary_spellings() {
        assert_eq!(XSD_STRING, "http://www.w3.org/2001/XMLSchema#string");
        assert_eq!(
            RDF_LANG_STRING,
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
        );
        assert!(XSD_STRING.starts_with(XSD));
        assert!(!RDF_LANG_STRING.starts_with(XSD));
    }
}
