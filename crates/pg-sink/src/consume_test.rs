use super::*;
use crate::batch::SystemClock;
use crate::reload_event::{
    FencePhase, FenceWaiters, PendingReloadEvent, PendingReloadEvents, ReloadEventKind,
    ReloadScope, ReloadTarget,
};
use arrow::array::{Array, Int32Array, StringArray};
use common::{PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo, SinkMeta, TupleValue};
use pg_to_arrow::oids;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use uuid::Uuid;

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

#[test]
fn malformed_reload_event_is_a_decode_error() {
    let rel = PgRelation {
        oid: 91,
        schema: "walrus".into(),
        name: "reload_event".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: Vec::new(),
    };

    let error = decode_reload_event(&rel, &[], None, None).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("parse walrus.reload_event tuple"),
        "malformed source control rows must stop decoding: {error:#}"
    );
}

#[test]
fn ddl_tracking_uses_oid_and_never_qualified_name_aliasing() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(false), SchemaVersionNo(1))
        .unwrap();

    let mut event = crate::ddl::DdlEvent {
        source_audit_id: 1,
        capture_lsn: Lsn::new(100),
        c_event: "ddl_command_end".into(),
        c_tag: "ALTER TABLE".into(),
        source_schema: "public".into(),
        source_table: "orders".into(),
        c_rel_oid: Some(43),
        c_replica_identity: Some(ReplicaIdentity::Default),
        c_columns: Some(serde_json::json!([])),
        c_dropped: None,
        c_ddl_text: None,
    };

    assert!(
        tracked_relation_for_ddl(&cache, &event).is_none(),
        "a same-name relation with a different OID is not this epoch's tracked table"
    );

    event.c_rel_oid = Some(42);
    assert_eq!(
        tracked_relation_for_ddl(&cache, &event).unwrap().oid,
        42,
        "the exact frozen relation identity remains tracked"
    );
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

fn quiet_router() -> BatchRouter<Arc<SystemClock>> {
    BatchRouter::new(
        BatchTriggers {
            max_rows: NonZeroU64::MAX,
            max_bytes: NonZeroU64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        EpochNo(1),
        "test",
    )
}

fn batch_ops(batch: &SealedBatch) -> Vec<Op> {
    let meta = batch
        .record_batch
        .column(batch.record_batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..meta.len())
        .map(|row| {
            serde_json::from_str::<SinkMeta>(meta.value(row))
                .unwrap()
                .op
        })
        .collect()
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

#[test]
fn key_changing_update_emits_old_key_delete_then_new_image() {
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
                xid: 7,
            },
            Lsn::new(100),
            SchemaVersionNo(1),
        )
        .unwrap();
    router
        .route(
            &cache,
            &Message::Update {
                xid: None,
                relation_oid: 42,
                old_kind: Some(crate::pgoutput::OldTupleKind::Key),
                old: Some(vec![TupleValue::Text("1".into()), TupleValue::Null]),
                new: vec![
                    TupleValue::Text("2".into()),
                    TupleValue::Text("moved".into()),
                ],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap();
    let mut sealed = router
        .route(
            &cache,
            &Message::Commit {
                flags: 0,
                commit_lsn: Lsn::new(120),
                end_lsn: Lsn::new(121),
                commit_ts: 0,
            },
            Lsn::new(120),
            SchemaVersionNo(1),
        )
        .unwrap();

    assert_eq!(sealed.len(), 1);
    let batch = sealed.pop().unwrap();
    assert_eq!(batch.row_count, 2);
    assert_eq!(batch_ops(&batch), vec![Op::Delete, Op::Update]);
    let ids = batch
        .record_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!((ids.value(0), ids.value(1)), (1, 2));
}

#[test]
fn update_with_unchanged_key_emits_only_new_image() {
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
                xid: 7,
            },
            Lsn::new(100),
            SchemaVersionNo(1),
        )
        .unwrap();
    router
        .route(
            &cache,
            &Message::Update {
                xid: None,
                relation_oid: 42,
                old_kind: Some(crate::pgoutput::OldTupleKind::Full),
                old: Some(vec![
                    TupleValue::Text("1".into()),
                    TupleValue::Text("before".into()),
                ]),
                new: vec![
                    TupleValue::Text("1".into()),
                    TupleValue::Text("after".into()),
                ],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap();
    let mut sealed = router
        .route(
            &cache,
            &Message::Commit {
                flags: 0,
                commit_lsn: Lsn::new(120),
                end_lsn: Lsn::new(121),
                commit_ts: 0,
            },
            Lsn::new(120),
            SchemaVersionNo(1),
        )
        .unwrap();

    let batch = sealed.pop().unwrap();
    assert_eq!(batch.row_count, 1);
    assert_eq!(batch_ops(&batch), vec![Op::Update]);
}

#[test]
fn ordinary_update_normalizes_key_toast_and_records_non_key_toast() {
    let mut relation = orders_relation(false);
    relation.replica_identity = ReplicaIdentity::Index;
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(relation, SchemaVersionNo(1))
        .unwrap();
    let mut router = router();
    router.bind_relation(42, SchemaVersionNo(1));
    router
        .route(
            &cache,
            &Message::Begin {
                final_lsn: Lsn::new(100),
                commit_ts: 0,
                xid: 7,
            },
            Lsn::new(100),
            SchemaVersionNo(1),
        )
        .unwrap();
    router
        .route(
            &cache,
            &Message::Update {
                xid: None,
                relation_oid: 42,
                old_kind: Some(crate::pgoutput::OldTupleKind::Key),
                old: Some(vec![TupleValue::Text("1".into()), TupleValue::Null]),
                new: vec![TupleValue::UnchangedToast, TupleValue::UnchangedToast],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap();
    let mut sealed = router
        .route(
            &cache,
            &Message::Commit {
                flags: 0,
                commit_lsn: Lsn::new(120),
                end_lsn: Lsn::new(121),
                commit_ts: 0,
            },
            Lsn::new(120),
            SchemaVersionNo(1),
        )
        .unwrap();

    let batch = sealed.pop().unwrap();
    assert_eq!(batch.row_count, 1, "the resolved key did not move");
    let ids = batch
        .record_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1, "key sentinel was replaced from old image");
    let meta = batch
        .record_batch
        .column(batch.record_batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let meta: SinkMeta = serde_json::from_str(meta.value(0)).unwrap();
    assert_eq!(meta.unchanged_toast.as_ref(), &["note".to_string()]);
}

#[test]
fn ordinary_truncate_becomes_a_durable_table_boundary_row() {
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
                xid: 7,
            },
            Lsn::new(100),
            SchemaVersionNo(1),
        )
        .unwrap();
    router
        .route(
            &cache,
            &Message::Truncate {
                xid: None,
                cascade: false,
                restart_identity: false,
                relations: vec![42],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap();
    let mut sealed = router
        .route(
            &cache,
            &Message::Commit {
                flags: 0,
                commit_lsn: Lsn::new(120),
                end_lsn: Lsn::new(121),
                commit_ts: 0,
            },
            Lsn::new(120),
            SchemaVersionNo(1),
        )
        .unwrap();

    let batch = sealed.pop().unwrap();
    assert_eq!(batch.row_count, 1);
    assert_eq!(batch_ops(&batch), vec![Op::Truncate]);
    let ids = batch
        .record_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(ids.is_null(0), "truncate carries no synthetic key value");
}

#[test]
fn truncate_with_unknown_relation_fails_instead_of_being_ignored() {
    let cache = RelationCache::default();
    let mut router = router();
    let error = router
        .route(
            &cache,
            &Message::Truncate {
                xid: None,
                cascade: false,
                restart_identity: false,
                relations: vec![999],
            },
            Lsn::new(110),
            SchemaVersionNo(1),
        )
        .unwrap_err();
    assert!(error.to_string().contains("oid=999"));
}

#[test]
fn force_flush_table_seals_only_the_target_and_keeps_other_ack_floor() {
    let mut cache = RelationCache::default();
    cache
        .upsert_from_relation(orders_relation(false), SchemaVersionNo(1))
        .unwrap();
    let mut invoices = orders_relation(false);
    invoices.oid = 43;
    invoices.name = "invoices".into();
    cache
        .upsert_from_relation(invoices, SchemaVersionNo(1))
        .unwrap();

    let mut router = quiet_router();
    router.bind_relation(42, SchemaVersionNo(1));
    router.bind_relation(43, SchemaVersionNo(1));
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
    for (relation_oid, id) in [(42, "1"), (43, "2")] {
        router
            .route(
                &cache,
                &Message::Insert {
                    xid: None,
                    relation_oid,
                    new: vec![TupleValue::Text(id.into()), TupleValue::Text("row".into())],
                },
                Lsn::new(110),
                SchemaVersionNo(1),
            )
            .unwrap();
    }
    assert!(
        router
            .route(
                &cache,
                &Message::Commit {
                    flags: 0,
                    commit_lsn: Lsn::new(200),
                    end_lsn: Lsn::new(201),
                    commit_ts: 0,
                },
                Lsn::new(200),
                SchemaVersionNo(1),
            )
            .unwrap()
            .is_empty(),
        "both committed tables remain below their ordinary triggers"
    );

    let target = router.force_flush_table("public", "orders").unwrap();
    assert_eq!(target.len(), 1);
    assert_eq!(target[0].table, "orders");
    assert_eq!(target[0].lsn_end, Lsn::new(200));
    assert_eq!(
        router.undurable_floor(),
        Some(Lsn::new(200)),
        "the unrelated committed table still fences confirmed_flush"
    );
    assert!(
        router
            .force_flush_table("public", "orders")
            .unwrap()
            .is_empty(),
        "a repeated fence is idempotent in memory"
    );

    let other = router.force_flush_table("public", "invoices").unwrap();
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].table, "invoices");
    assert_eq!(router.undurable_floor(), None);
}

#[tokio::test]
async fn committed_end_fence_resolves_only_from_the_persisted_state() {
    let reload_id = common::ReloadId(91);
    let request_id = Uuid::new_v4();
    let waiters = FenceWaiters::default();
    let mut waiter = Box::pin(waiters.subscribe(reload_id, FencePhase::End));
    let mut pending = PendingReloadEvents::default();
    pending.push(PendingReloadEvent {
        event_id: Uuid::new_v4(),
        request_id,
        reload_id: Some(reload_id),
        kind: ReloadEventKind::EndFence,
        scope: ReloadScope::Table,
        source_schema: Some("public".into()),
        source_table: Some("orders".into()),
        targets: Vec::new(),
        schema_version: Some(SchemaVersionNo(1)),
        embedded_lsn: Lsn::new(250),
        xid: None,
        top_xid: None,
    });

    let committed = pending.on_commit(Lsn::new(300));
    assert_eq!(
        waiters.waiter_count(),
        1,
        "transaction commit alone must not resolve an end fence"
    );
    tokio::select! {
        biased;
        result = &mut waiter => panic!("end fence resolved before durability: {result:?}"),
        () = tokio::task::yield_now() => {}
    }

    // Production can construct this state only after target PUT + manifest + end-marker commit.
    let requests = PersistedReloadEvents { events: committed }.resolve_fences(&waiters);
    assert!(requests.is_empty());
    let echo = waiter.await.unwrap();
    assert_eq!(echo.commit_lsn, Lsn::new(300));
    assert_eq!(echo.embedded_lsn, Lsn::new(250));
}

#[test]
fn only_failed_attempt_with_exact_target_is_a_stale_fence_noop() {
    let request_id = Uuid::new_v4();
    let version = SchemaVersionNo(3);
    let attempt = FailedReloadAttempt {
        status: control::ReloadStatus::Failed,
        target: ("public", "orders"),
        request_id: Some(request_id),
        schema_version: Some(version),
    };
    let fence = DecodedFenceIdentity {
        target: Some(("public", "orders")),
        request_id,
        schema_version: Some(version),
    };

    assert!(is_matching_failed_fence(attempt, fence));
    assert!(!is_matching_failed_fence(
        FailedReloadAttempt {
            status: control::ReloadStatus::Exporting,
            ..attempt
        },
        fence,
    ));
    assert!(!is_matching_failed_fence(
        attempt,
        DecodedFenceIdentity {
            target: Some(("public", "invoices")),
            ..fence
        },
    ));
    assert!(!is_matching_failed_fence(
        attempt,
        DecodedFenceIdentity {
            target: None,
            ..fence
        },
    ));
    assert!(!is_matching_failed_fence(
        attempt,
        DecodedFenceIdentity {
            request_id: Uuid::new_v4(),
            ..fence
        },
    ));
    assert!(!is_matching_failed_fence(
        attempt,
        DecodedFenceIdentity {
            schema_version: Some(SchemaVersionNo(4)),
            ..fence
        },
    ));
}

#[test]
fn all_published_fanout_uses_and_dedupes_the_frozen_event_inventory() {
    let targets = vec![
        ReloadTarget {
            schema: "public".into(),
            table: "orders".into(),
        },
        ReloadTarget {
            schema: "sales".into(),
            table: "invoices".into(),
        },
        ReloadTarget {
            schema: "public".into(),
            table: "orders".into(),
        },
    ];

    assert_eq!(
        dedupe_reload_targets(&targets)
            .into_iter()
            .collect::<Vec<_>>(),
        vec![("public", "orders"), ("sales", "invoices")]
    );
}
