use super::*;

#[test]
fn slice_borrows_without_copying_and_advances() {
    let input = b"abcdef";
    let mut reader = Reader::new(input);

    let borrowed = reader.slice(3).unwrap();

    assert_eq!(borrowed, b"abc");
    assert_eq!(borrowed.as_ptr(), input.as_ptr());
    assert_eq!(reader.remaining(), 3);
}

#[test]
fn str_rejects_invalid_utf8_as_a_decode_error() {
    let mut reader = Reader::new(&[0xff]);

    assert!(matches!(reader.str(1), Err(DecodeError::Utf8(_))));
}

#[test]
fn slice_past_the_end_is_unexpected_eof() {
    let mut reader = Reader::new(b"ab");
    assert_eq!(reader.byte1().unwrap(), b'a');

    assert!(matches!(
        reader.slice(3),
        Err(DecodeError::UnexpectedEof {
            needed: 3,
            offset: 1,
            remaining: 1,
        })
    ));
}
