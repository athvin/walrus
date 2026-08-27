const ROOT: &str = include_str!("../../../Cargo.toml");
const CONTROL_DB: &str = include_str!("../../control/src/db.rs");
const SINK_CONSUME: &str = include_str!("../../pg-sink/src/consume.rs");
const ARROW_BATCH: &str = include_str!("../../pg-to-arrow/src/batch.rs");

#[test]
fn workspace_is_edition_2024_for_if_let_chains() {
    assert!(ROOT.contains("edition = \"2024\" # if-let chains"));
    assert!(ROOT.contains("resolver = \"2\""));
    assert!(ROOT.contains("rust-version = \"1.95\""));
}

#[test]
fn the_four_audited_nests_are_chains() {
    assert!(
        CONTROL_DB
            .contains("if let sqlx::Error::Database(db) = &e\n            && db.code().as_deref()")
    );
    assert!(SINK_CONSUME.contains(
        "if let Some(mut batcher) = self.batchers.remove(&oid)\n            && let Some(batch)"
    ));
    assert!(ARROW_BATCH.contains(
        "if let DataType::List(item) = field.data_type()\n        && let DataType::Struct(fs)"
    ));
    assert!(ARROW_BATCH.contains("&& let Some(bound) = fs.first()"));
    assert!(ARROW_BATCH.contains("if let Some(t) = scratch.find('T')\n        && let Some(sign)"));

    let audited = [CONTROL_DB, SINK_CONSUME, ARROW_BATCH].concat();
    assert!(audited.matches("&& let ").count() >= 4);
}
