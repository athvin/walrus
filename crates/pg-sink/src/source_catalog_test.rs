use super::*;

#[test]
fn out_of_range_catalog_oid_is_reported_not_wrapped() {
    let raw = i64::from(u32::MAX) + 1;
    let err = catalog_oid(raw).unwrap_err();
    let message = format!("{err:#}");
    assert!(
        message.contains(&raw.to_string()),
        "error includes {raw}: {message}"
    );
}

#[test]
fn publication_actions_require_all_four_mutation_families() {
    let complete = PublicationActions {
        insert: true,
        update: true,
        delete: true,
        truncate: true,
    };
    assert!(require_publication_actions("walrus_pub", Some(complete)).is_ok());

    let incomplete = PublicationActions {
        delete: false,
        ..complete
    };
    let err = require_publication_actions("walrus_pub", Some(incomplete)).unwrap_err();
    assert!(matches!(
        err,
        PublicationCoverageIssue::DisabledOperations { .. }
    ));
    assert!(err.to_string().contains("DELETE"));
}

#[test]
fn target_restrictions_are_rejected_independently() {
    let full = PublicationTargetOptions {
        published: true,
        row_filter: false,
        column_list: false,
        topology_stable: true,
    };
    assert!(require_full_target("walrus_pub", "public", "orders", full).is_ok());

    let filtered = PublicationTargetOptions {
        row_filter: true,
        ..full
    };
    assert!(matches!(
        require_full_target("walrus_pub", "public", "orders", filtered),
        Err(PublicationCoverageIssue::RowFilter { .. })
    ));

    let projected = PublicationTargetOptions {
        column_list: true,
        ..full
    };
    assert!(matches!(
        require_full_target("walrus_pub", "public", "orders", projected),
        Err(PublicationCoverageIssue::ColumnList { .. })
    ));

    let absent = PublicationTargetOptions {
        published: false,
        ..full
    };
    assert!(matches!(
        require_full_target("walrus_pub", "public", "orders", absent),
        Err(PublicationCoverageIssue::MissingTarget { .. })
    ));

    let topology_dependent = PublicationTargetOptions {
        topology_stable: false,
        ..full
    };
    assert!(matches!(
        require_full_target("walrus_pub", "public", "orders", topology_dependent),
        Err(PublicationCoverageIssue::TopologyDependent { .. })
    ));
}

#[test]
fn advisory_key_matches_the_source_migration_literal() {
    assert_eq!(PUBLICATION_DDL_GUARD_KEY, 8_602_276_002_106_929_250);
}
