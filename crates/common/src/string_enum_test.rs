#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestParseError {
    column: &'static str,
    input: String,
}

impl TestParseError {
    fn new(column: &'static str, input: &str) -> Self {
        Self {
            column,
            input: input.to_string(),
        }
    }
}

string_enum! {
    /// A throwaway enum that exercises every generated item without touching a real contract.
    Signal {
        error = TestParseError;
        column = "test.signal";
        Green => "green",
        Amber => "amber",
        Red   => "red", // trailing comma must be accepted by the `$(,)?` in the matcher
    }
}

#[test]
fn round_trips_every_variant_both_directions() {
    for (v, s) in [
        (Signal::Green, "green"),
        (Signal::Amber, "amber"),
        (Signal::Red, "red"),
    ] {
        assert_eq!(v.as_str(), s);
        assert_eq!(s.parse::<Signal>(), Ok(v));
    }
}

#[test]
fn rejection_keeps_the_supplied_column_and_input_as_data() {
    let err = "chartreuse".parse::<Signal>().unwrap_err();
    assert_eq!(err.column, "test.signal");
    assert_eq!(err.input, "chartreuse");
}

#[test]
fn generated_enum_is_copy_and_eq() {
    let a = Signal::Amber;
    let b = a; // `a` is still usable => Copy, which `as_str(self)` requires.
    assert_eq!(a, b);
    assert_eq!(format!("{a:?}"), "Amber");
}
