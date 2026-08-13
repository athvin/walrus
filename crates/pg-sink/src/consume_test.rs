#[test]
fn build_names_the_first_missing_field() {
    // The DecodeLoopBuilder compile-fail doctest is the real dropped-setter regression; a runtime
    // test cannot express a compile-time unused-result error.
    let Err(error) = super::DecodeLoop::builder().build() else {
        panic!("an empty builder must reject its first missing field");
    };
    assert_eq!(
        error.to_string(),
        "decode loop builder: missing required field `stream`"
    );
}
