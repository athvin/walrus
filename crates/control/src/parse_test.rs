use super::*;

#[test]
fn display_quotes_the_rejected_input() {
    let error = ParseEnumError::new("manifest kind", "snapshottt");
    assert_eq!(error.to_string(), "invalid manifest kind: \"snapshottt\"");
}

#[test]
fn display_is_unambiguous_for_whitespace_and_empty_input() {
    let whitespace = ParseEnumError::new("reload status", " complete ");
    assert_eq!(
        whitespace.to_string(),
        "invalid reload status: \" complete \""
    );

    let empty = ParseEnumError::new("manifest status", "");
    assert_eq!(empty.to_string(), "invalid manifest status: \"\"");
}
