//! Conservative startup collection for staging objects left between durable PUT and manifest commit.
//!
//! Only the current epoch's exact sink-generated key shape is eligible. A durable manifest URI,
//! recent modification time, foreign epoch, system prefix, DuckLake prefix, or unfamiliar layout
//! always retains the object. This collector runs after this process has claimed the configured
//! replication slot, but before it starts decoding or exporting.

use anyhow::Context;
use common::EpochNo;
use futures_util::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use sqlx::PgPool;
use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A completed PUT can precede its control transaction. Twenty-four hours is intentionally much
/// longer than an ordinary publication attempt while still reclaiming crash debris predictably.
const ORPHAN_GRACE_PERIOD: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectDisposition {
    IgnoreUnknown,
    RetainReferenced,
    RetainYoung,
    DeleteOrphan,
}

/// Observable result of one startup sweep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrphanCleanupStats {
    pub scanned: u64,
    pub ignored: u64,
    pub referenced: u64,
    pub young: u64,
    pub deleted: u64,
}

/// Read the epoch's complete durable manifest inventory, then delete old unreferenced staging
/// objects. The caller must already own the active source replication slot, and no writer task in
/// this process may start before this future completes.
///
/// # Errors
///
/// Returns an error if manifest inventory, object listing, URI validation, or deletion fails. A
/// failure stops startup rather than continuing from a partially trusted reference set.
pub(crate) async fn cleanup_epoch_orphans(
    store: &dyn ObjectStore,
    bucket: &str,
    pool: &PgPool,
    epoch: EpochNo,
) -> anyhow::Result<OrphanCleanupStats> {
    let manifest_uris = control::list_manifest_uris(pool, epoch)
        .await
        .context("read durable manifest URI inventory for orphan cleanup")?;
    let referenced = referenced_keys(bucket, &manifest_uris)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs();
    let cutoff =
        i64::try_from(now.saturating_sub(ORPHAN_GRACE_PERIOD.as_secs())).unwrap_or(i64::MAX);

    let stats = sweep_epoch_at(store, epoch, &referenced, cutoff).await?;
    tracing::info!(
        %epoch,
        grace_seconds = ORPHAN_GRACE_PERIOD.as_secs(),
        scanned = stats.scanned,
        ignored = stats.ignored,
        referenced = stats.referenced,
        young = stats.young,
        deleted = stats.deleted,
        "startup orphan-object sweep complete"
    );
    Ok(stats)
}

fn referenced_keys(bucket: &str, manifest_uris: &[String]) -> anyhow::Result<HashSet<String>> {
    let prefix = format!("s3://{bucket}/");
    manifest_uris
        .iter()
        .map(|uri| {
            let key = uri.strip_prefix(&prefix).with_context(|| {
                format!("manifest URI {uri:?} is outside configured bucket {bucket:?}")
            })?;
            anyhow::ensure!(!key.is_empty(), "manifest URI {uri:?} has no object key");
            Ok(key.to_owned())
        })
        .collect()
}

async fn sweep_epoch_at(
    store: &dyn ObjectStore,
    epoch: EpochNo,
    referenced: &HashSet<String>,
    cutoff_unix_seconds: i64,
) -> anyhow::Result<OrphanCleanupStats> {
    let prefix = Path::from(epoch.to_string());
    let mut objects = store.list(Some(&prefix));
    let mut stats = OrphanCleanupStats::default();
    while let Some(meta) = objects.next().await {
        let meta = meta.context("list epoch object during orphan cleanup")?;
        stats.scanned = stats.scanned.saturating_add(1);
        match classify_epoch_object(
            epoch,
            &meta.location,
            meta.last_modified.timestamp(),
            cutoff_unix_seconds,
            referenced,
        ) {
            ObjectDisposition::IgnoreUnknown => {
                stats.ignored = stats.ignored.saturating_add(1);
            }
            ObjectDisposition::RetainReferenced => {
                stats.referenced = stats.referenced.saturating_add(1);
            }
            ObjectDisposition::RetainYoung => {
                stats.young = stats.young.saturating_add(1);
            }
            ObjectDisposition::DeleteOrphan => {
                store
                    .delete(&meta.location)
                    .await
                    .with_context(|| format!("delete orphan object {}", meta.location))?;
                stats.deleted = stats.deleted.saturating_add(1);
                tracing::info!(key = %meta.location, %epoch, "deleted old unreferenced staging object");
            }
        }
    }
    Ok(stats)
}

fn classify_epoch_object(
    epoch: EpochNo,
    location: &Path,
    modified_unix_seconds: i64,
    cutoff_unix_seconds: i64,
    referenced: &HashSet<String>,
) -> ObjectDisposition {
    if !is_sink_data_key(epoch, location) {
        return ObjectDisposition::IgnoreUnknown;
    }
    if referenced.contains(location.as_ref()) {
        return ObjectDisposition::RetainReferenced;
    }
    if modified_unix_seconds >= cutoff_unix_seconds {
        return ObjectDisposition::RetainYoung;
    }
    ObjectDisposition::DeleteOrphan
}

/// Positive recognition of `<epoch>/<schema>/<table>/<16-hex-lsn>-<v4-uuid>.parquet` keeps this
/// collector out of DuckLake/system namespaces and makes an unfamiliar future layout leak safely.
fn is_sink_data_key(epoch: EpochNo, location: &Path) -> bool {
    let expected_epoch = epoch.to_string();
    let mut parts = location.as_ref().split('/');
    let (Some(key_epoch), Some(schema), Some(table), Some(file)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    if parts.next().is_some()
        || key_epoch != expected_epoch
        || schema.is_empty()
        || table.is_empty()
    {
        return false;
    }
    let Some(stem) = file.strip_suffix(".parquet") else {
        return false;
    };
    let Some((lsn, uuid_text)) = stem.split_once('-') else {
        return false;
    };
    if lsn.len() != 16
        || !lsn
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'A'..=b'F').contains(&byte))
    {
        return false;
    }
    let Ok(id) = uuid::Uuid::parse_str(uuid_text) else {
        return false;
    };
    id.get_version() == Some(uuid::Version::Random) && id.hyphenated().to_string() == uuid_text
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    const UUID: &str = "c0a8012e-7a6f-4b48-9b5b-6023f5f1bb2d";

    fn data_key(epoch: i64, table: &str) -> Path {
        Path::from(format!(
            "{epoch}/public/{table}/0000000000000100-{UUID}.parquet"
        ))
    }

    #[test]
    fn classifier_deletes_only_exact_old_unreferenced_sink_keys() {
        let epoch = EpochNo(7);
        let key = data_key(7, "orders");
        let referenced = HashSet::from([key.to_string()]);

        assert_eq!(
            classify_epoch_object(epoch, &key, 1, 100, &referenced),
            ObjectDisposition::RetainReferenced
        );
        assert_eq!(
            classify_epoch_object(epoch, &key, 100, 100, &HashSet::new()),
            ObjectDisposition::RetainYoung,
            "the exact grace boundary is retained"
        );
        assert_eq!(
            classify_epoch_object(epoch, &key, 99, 100, &HashSet::new()),
            ObjectDisposition::DeleteOrphan
        );

        for ignored in [
            data_key(8, "orders"),
            Path::from("_walrus/canary/pod-a"),
            Path::from("ducklake/prod/main/ducklake-1.parquet"),
            Path::from(format!("7/public/orders/not-an-lsn-{UUID}.parquet")),
            Path::from("7/public/orders/0000000000000100-not-a-uuid.parquet"),
        ] {
            assert_eq!(
                classify_epoch_object(epoch, &ignored, 1, 100, &HashSet::new()),
                ObjectDisposition::IgnoreUnknown,
                "ignored {ignored}"
            );
        }
    }

    #[test]
    fn manifest_uri_inventory_must_match_the_configured_bucket() {
        assert_eq!(
            referenced_keys(
                "walrus",
                &["s3://walrus/7/public/orders/file.parquet".into()]
            )
            .unwrap(),
            HashSet::from(["7/public/orders/file.parquet".to_string()])
        );
        assert!(
            referenced_keys(
                "walrus",
                &["s3://another/7/public/orders/file.parquet".into()]
            )
            .is_err(),
            "an incomplete reference inventory must fail closed"
        );
    }

    #[tokio::test]
    async fn in_memory_sweep_deletes_only_the_matching_unreferenced_object() {
        let store = InMemory::new();
        let orphan = data_key(7, "orphan");
        let referenced_key = data_key(7, "referenced");
        let unfamiliar = Path::from("7/ducklake/data/ducklake-file.parquet");
        let other_epoch = data_key(8, "other_epoch");
        let system = Path::from("_walrus/canary/pod-a");
        for key in [&orphan, &referenced_key, &unfamiliar, &other_epoch, &system] {
            store.put(key, PutPayload::from_static(b"x")).await.unwrap();
        }

        let referenced = HashSet::from([referenced_key.to_string()]);
        let stats = sweep_epoch_at(&store, EpochNo(7), &referenced, i64::MAX)
            .await
            .unwrap();

        assert_eq!(stats.scanned, 3);
        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.referenced, 1);
        assert_eq!(stats.ignored, 1);
        assert!(store.head(&orphan).await.is_err());
        for retained in [&referenced_key, &unfamiliar, &other_epoch, &system] {
            store.head(retained).await.unwrap();
        }
    }
}
