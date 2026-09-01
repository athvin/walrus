use super::*;
use crate::batch::SystemClock;
use common::{PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo, TupleValue};
use pg_to_arrow::oids;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

fn orders_relation(with_extra: bool) -> PgRelation {
    let mut columns = vec![
        PgColumn {
            name: "id".into(),
            type_oid: oids::INT4,
            type_modifier: -1,
            is_key: true,
        },
        PgColumn {
            name: "note".into(),
            type_oid: oids::TEXT,
            type_modifier: -1,
            is_key: false,
        },
    ];
    if with_extra {
        columns.push(PgColumn {
            name: "extra".into(),
            type_oid: oids::TEXT,
            type_modifier: -1,
            is_key: false,
        });
    }
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns,
    }
}

fn router() -> BatchRouter<Arc<SystemClock>> {
    BatchRouter::new(
        BatchTriggers {
            max_rows: NonZeroU64::MIN,
            max_bytes: NonZeroU64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        EpochNo(1),
        "test",
    )
}

#[test]
fn build_names_the_first_missing_field() {
    // The DecodeLoopBuilder compile-fail doctest is the real dropped-setter regression; a runtime
    // test cannot express a compile-time unused-result error.
    let Err(error) = DecodeLoop::<Arc<SystemClock>>::builder().build() else {
        panic!("an empty builder must reject its first missing field");
    };
    assert_eq!(
        error.to_string(),
        "decode loop builder: missing required field `stream`"
    );
}

/// Stands in for a frame future that makes partial progress before completing.
async fn stepwise(progress: &AtomicU32, steps: u32) {
    for _ in 0..steps {
        tokio::time::sleep(Duration::from_millis(10)).await;
        progress.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test(start_paused = true)]
async fn pinned_branch_survives_other_arm_firing() {
    let progress = AtomicU32::new(0);
    let frame = stepwise(&progress, 5);
    tokio::pin!(frame);
    let mut ticker = tokio::time::interval(Duration::from_millis(3));
    let mut interruptions = 0_u32;
    loop {
        tokio::select! {
            () = &mut frame => break,
            _ = ticker.tick() => interruptions += 1,
        }
    }
    assert!(interruptions > 0, "the sibling arm must win at least once");
    assert_eq!(
        progress.load(Ordering::Relaxed),
        5,
        "partial progress must survive"
    );
}

#[tokio::test(start_paused = true)]
async fn recreated_branch_loses_progress() {
    let progress = AtomicU32::new(0);
    let mut ticker = tokio::time::interval(Duration::from_millis(3));
    let mut interruptions = 0_u32;
    for _ in 0..10 {
        tokio::select! {
            () = stepwise(&progress, 5) => break,
            _ = ticker.tick() => interruptions += 1,
        }
    }
    assert_eq!(interruptions, 10, "the sibling arm must keep interrupting");
    assert_eq!(
        progress.load(Ordering::Relaxed),
        0,
        "recreating the future loses progress"
    );
}

#[test]
fn ordinary_ddl_cut_preserves_pre_ddl_rows_and_separates_schema_versions() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(false), SchemaVersionNo(1))
        .unwrap();
    let mut router = router();
    router.bind_relation(42, SchemaVersionNo(1));

    router
        .route(
            &cache,
            &Message::Begin {
                final_lsn: Lsn::new(100),
                commit_ts: 0,
                xid: 700,
            },
            Lsn::new(100),
            SchemaVersionNo(1),
        )
        .unwrap();
    router
        .route(
            &cache,
            &Message::Insert {
                xid: None,
                relation_oid: 42,
                new: vec![TupleValue::Text("1".into()), TupleValue::Text("old".into())],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap();

    assert!(
        router
            .cut_table(&cache, "public", "orders")
            .unwrap()
            .is_empty(),
        "an open transaction cannot be sealed at its DDL event"
    );
    assert_eq!(router.pending_cuts.len(), 1);

    cache
        .upsert_from_relation(orders_relation(true), SchemaVersionNo(2))
        .unwrap();
    router.bind_relation(42, SchemaVersionNo(2));
    router
        .route(
            &cache,
            &Message::Insert {
                xid: None,
                relation_oid: 42,
                new: vec![
                    TupleValue::Text("2".into()),
                    TupleValue::Text("new".into()),
                    TupleValue::Text("v2".into()),
                ],
            },
            Lsn::new(120),
            SchemaVersionNo(2),
        )
        .unwrap();

    let sealed = router
        .route(
            &cache,
            &Message::Commit {
                flags: 0,
                commit_lsn: Lsn::new(900),
                end_lsn: Lsn::new(901),
                commit_ts: 0,
            },
            Lsn::new(900),
            SchemaVersionNo(2),
        )
        .unwrap();

    assert_eq!(sealed.len(), 2);
    assert_eq!(
        sealed
            .iter()
            .map(|batch| (batch.schema_version, batch.row_count, batch.lsn_end))
            .collect::<Vec<_>>(),
        vec![
            (SchemaVersionNo(1), 1, Lsn::new(900)),
            (SchemaVersionNo(2), 1, Lsn::new(900)),
        ]
    );
    assert!(router.pending_cuts.is_empty());
}

#[test]
fn durable_frontier_waits_for_every_older_or_equal_unsealed_commit() {
    let durable = Some(Lsn::new(900));
    assert_eq!(durable_frontier(durable, None), durable);
    assert_eq!(
        durable_frontier(durable, Some(Lsn::new(901))),
        durable,
        "a later unsealed commit does not block an earlier durable group"
    );
    assert_eq!(
        durable_frontier(durable, Some(Lsn::new(900))),
        None,
        "a sibling at the same commit LSN must be durable before acknowledgement"
    );
    assert_eq!(
        durable_frontier(durable, Some(Lsn::new(800))),
        None,
        "an older unsealed commit fences the slot"
    );
    assert_eq!(durable_frontier(None, Some(Lsn::new(800))), None);
}
