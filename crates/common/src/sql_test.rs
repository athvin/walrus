use super::*;
use std::borrow::Cow;

#[test]
fn doubles_single_quotes() {
    assert_eq!(sql_literal("O'Brien"), "O''Brien");
    // Every quote is doubled, including a run of them.
    assert_eq!(sql_literal("''"), "''''");
    assert_eq!(sql_literal("a'b'c"), "a''b''c");
}

#[test]
fn leaves_clean_strings_untouched() {
    assert_eq!(sql_literal("plain text 123"), "plain text 123");
    // Double quotes are an identifier concern, not a literal one — left alone.
    assert_eq!(sql_literal("a\"b"), "a\"b");
}

#[test]
fn empty_string_is_empty() {
    assert_eq!(sql_literal(""), "");
}

#[test]
fn clean_input_is_borrowed_not_allocated() {
    assert!(matches!(sql_literal("plain"), Cow::Borrowed(_)));
}

#[test]
fn quoted_input_is_owned() {
    assert!(matches!(sql_literal("O'Brien"), Cow::Owned(_)));
}

#[test]
fn quoted_literal_adds_the_surrounding_quotes() {
    assert_eq!("wal_level".quoted_literal(), "'wal_level'");
    assert_eq!("O'Brien".quoted_literal(), "'O''Brien'");
    // An empty value is still a well-formed literal, not an empty statement fragment.
    assert_eq!("".quoted_literal(), "''");
}

#[test]
fn quoted_literal_matches_the_wrappers_it_replaced() {
    // `preflight::lit` and `reload_export::sql_lit` were both exactly this expression.
    for input in ["wal_level", "O'Brien", "''", "", "a\"b"] {
        let expected = format!("'{}'", input.replace('\'', "''"));
        assert_eq!(input.quoted_literal(), expected, "input {input:?}");
    }
}

#[test]
fn quoted_literal_reaches_owned_and_borrowed_receivers() {
    // `&String` and `Cow<'_, str>` call sites resolve through deref, so no caller needs `as_str()`.
    let owned = String::from("a'b");
    let borrowed: Cow<'_, str> = Cow::Borrowed("a'b");
    assert_eq!(owned.quoted_literal(), "'a''b'");
    assert_eq!(borrowed.quoted_literal(), "'a''b'");
}

#[test]
fn ident_doubles_interior_double_quotes() {
    assert_eq!(SqlIdent::new("a\"b").unwrap().to_string(), "\"a\"\"b\"");
    assert_eq!(SqlIdent::new("plain").unwrap().to_string(), "\"plain\"");
    assert_eq!(SqlIdent::new("\"\"").unwrap().to_string(), "\"\"\"\"\"\"");
    assert_eq!(SqlIdent::new("O'Brien").unwrap().to_string(), "\"O'Brien\"");
}

#[test]
fn ident_rejects_the_unrepresentable() {
    assert_eq!(SqlIdent::new(""), Err(IdentError::Empty));
    assert!(matches!(
        SqlIdent::new("a\0b"),
        Err(IdentError::InteriorNul(input)) if input == "a\0b"
    ));
}

#[test]
fn as_raw_returns_the_unquoted_name() {
    assert_eq!(SqlIdent::new("a\"b").unwrap().as_raw(), "a\"b");
}

#[test]
fn ident_output_matches_what_the_old_hand_rolled_escapers_produced() {
    for input in ["t", "a\"b", "Mixed Case", "_walrus_lsn", "ünïcode"] {
        let expected = format!("\"{}\"", input.replace('"', "\"\""));
        assert_eq!(
            SqlIdent::new(input).unwrap().to_string(),
            expected,
            "input {input:?}"
        );
    }
}
