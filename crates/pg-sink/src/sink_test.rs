use super::*;
use object_store::local::LocalFileSystem;

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
    // …and must not lose the engine error *itself*: it stays in the chain, downcastable, so a
    // reporter can tell a footer write apart from an out-of-spec schema.
    let cause = std::error::Error::source(&error).expect("encode keeps the engine error");
    assert!(cause.is::<parquet::errors::ParquetError>());
    assert_eq!(cause.to_string(), rendered);
}

/// The store is a **trait** dependency (`Arc<dyn ObjectStore>`), so the failure branch is reachable
/// by injecting a *different real implementation of that trait* instead of standing up a broken S3:
/// `LocalFileSystem` answers `NotFound` for a key nothing ever wrote. The `InMemory` fixture above
/// cannot play this part — its `delete` is remove-or-ignore and always returns `Ok` — which is what
/// the trait seam buys that a concrete client would not.
///
/// Nothing is created, so nothing is cleaned up: the epoch-namespaced key carries a fresh uuid and
/// the store only ever tries (and fails) to remove it.
///
/// What this pins is the classification, not the transport. `ParquetSink::delete` is the best-effort
/// cleanup for an aborted streamed txn's speculative files (PR 2.30), and "best-effort" must mean
/// *reported*, never *swallowed*: a store refusal stays `SinkError::Store` →
/// `common::Error::ObjectStore` — the transient class — and is never folded into the terminal
/// `Encode` side asserted above.
#[tokio::test]
async fn a_store_failure_propagates_as_the_transient_store_class() {
    let store = LocalFileSystem::new_with_prefix(std::env::temp_dir())
        .expect("the system temp dir is an existing directory");
    let sink = ParquetSink::new(Arc::new(store), "walrus", common::EpochNo(5));
    let file_id = uuid::Uuid::new_v4().to_string();
    let absent = sink.object_key("public", "orders", "0/2A0".parse().unwrap(), &file_id);

    let error = sink.delete(&absent).await.unwrap_err();

    assert!(matches!(&error, SinkError::Store(_)));
    let classified = common::Error::from(error);
    assert!(matches!(classified, common::Error::ObjectStore(_)));
}
