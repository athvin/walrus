use super::*;

fn sink() -> ParquetSink {
    ParquetSink::new(
        Arc::new(object_store::memory::InMemory::new()),
        "walrus",
        common::EpochNo(5),
    )
}

#[test]
fn the_bucket_name_is_taken_as_str_or_string() {
    // `new` converts once internally, so a `&str` literal and an owned `String` are interchangeable
    // at the call site and land as the same stored bucket.
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let borrowed = ParquetSink::new(Arc::clone(&store), "walrus", common::EpochNo(5));
    let owned = ParquetSink::new(store, String::from("walrus"), common::EpochNo(5));

    assert_eq!(borrowed.bucket, "walrus");
    assert_eq!(owned.bucket, borrowed.bucket);
}

#[test]
fn object_key_is_epoch_namespaced_and_lsn_sortable() {
    let s = sink();
    let lsn: Lsn = "0/1A2B3C".parse().unwrap();
    let key = s.object_key("public", "orders", lsn, "abcd");
    // <epoch>/<schema>/<table>/<lsn_end 16-hex>-<uuid>.parquet
    assert_eq!(key.as_ref(), format!("5/public/orders/{lsn}-abcd.parquet"));
    assert_eq!(lsn.to_string().len(), 16, "lsn is zero-padded 16-hex");

    // Zero-padded 16-hex means byte-lexical order matches commit-LSN order.
    let lo = s.object_key("public", "orders", "0/100".parse().unwrap(), "u");
    let hi = s.object_key("public", "orders", "1/0".parse().unwrap(), "u");
    assert!(lo.as_ref() < hi.as_ref(), "keys sort by commit LSN");
}

#[test]
fn a_parquet_failure_converts_into_the_encode_class() {
    // The `?` on every writer call routes through this From impl, so the writer path must stay
    // Encode (→ ExitCode::Internal) and must not lose the engine's own message.
    let engine = parquet::errors::ParquetError::General("footer write failed".into());
    let rendered = engine.to_string();

    let error = SinkError::from(engine);

    assert!(matches!(&error, SinkError::Encode(_)));
    assert_eq!(error.to_string(), format!("parquet encode: {rendered}"));
}
