use super::{Reader, parse_tuple};
use common::TupleValue;

const ARRAYVEC_DECISION: &str =
    include_str!("../../../../docs/implementation/notes/rust-skills/mem-arrayvec.md");
const PG_MAX_COLUMNS: u16 = 1_600;

fn all_null_tuple(ncols: u16) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(usize::from(ncols) + 2);
    bytes.extend_from_slice(&ncols.to_be_bytes());
    bytes.resize(usize::from(ncols) + 2, b'n');
    bytes
}

#[test]
fn decodes_a_postgres_max_width_tuple_without_a_fixed_capacity_type() {
    assert!(ARRAYVEC_DECISION.contains("structural, not bench-gated"));
    let bytes = all_null_tuple(PG_MAX_COLUMNS);
    let mut reader = Reader::new(&bytes);

    let columns = parse_tuple(&mut reader).unwrap();

    assert_eq!(columns.len(), usize::from(PG_MAX_COLUMNS));
    assert!(columns.iter().all(|value| *value == TupleValue::Null));
    assert_eq!(reader.remaining(), 0);
    assert!(columns.capacity() >= usize::from(PG_MAX_COLUMNS));
}

#[test]
fn decodes_wider_than_the_postgres_ceiling_too() {
    let ncols = 2_000;
    let bytes = all_null_tuple(ncols);
    let mut reader = Reader::new(&bytes);

    let columns = parse_tuple(&mut reader).unwrap();

    assert_eq!(columns.len(), usize::from(ncols));
    assert_eq!(reader.remaining(), 0);
}
