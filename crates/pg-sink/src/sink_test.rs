use super::*;

fn sink() -> ParquetSink {
    ParquetSink::new(
        Arc::new(object_store::memory::InMemory::new()),
        "walrus".into(),
        5,
    )
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
