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
