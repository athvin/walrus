//! Golden-vector fixtures for the pgoutput decoder — a faithful, byte-for-byte port of
//! `docs/examples/proto-version/test_decode_pgoutput.py::VECTORS`.
//!
//! PR 2.1 only stands up the table + helpers and one `#[ignore]`d enumerating test; PRs 2.2–2.8
//! turn the vectors on family-by-family, TDD-style. `decode`/`render` are scaffolding those later
//! PRs invoke — unused until then, hence the crate-level allow.
#![allow(dead_code)]

use pg_sink::pgoutput::{parse_message, Message, StreamCtx};

/// A ported row of `VECTORS`: bytes in, the golden render line out.
pub struct Vector {
    pub name: &'static str,
    /// Message bytes, hex-encoded (lowercase, as captured).
    pub hex: &'static str,
    /// Were we inside a Stream Start..Stop block when this message arrived? (Only then does a
    /// per-message xid prefix exist.)
    pub streaming: bool,
    /// The golden render line, verbatim from the Python reference.
    pub expected: &'static str,
}

/// Every entry of `test_decode_pgoutput.py::VECTORS`, ported verbatim (names, hex, streaming flag,
/// expected line). `concat!` mirrors Python's adjacent-string-literal concatenation.
pub const VECTORS: &[Vector] = &[
    // ---- non-streamed transaction control + DML (REPLICA IDENTITY DEFAULT) ----
    Vector {
        name: "begin",
        hex: "42000000000199bac80002f8dc466a6b4f000002ed",
        streaming: false,
        expected:
            "BEGIN         final_lsn=0/199BAC8 commit_ts=2026-07-05T13:55:11.294287+00:00 xid=749",
    },
    Vector {
        name: "type_enum",
        hex: "59000040067075626c6963006d6f6f6400",
        streaming: false,
        expected: "TYPE          oid=16390 public.mood",
    },
    Vector {
        name: "relation_orders",
        hex: concat!(
            "520000400d7075626c6963006f7264657273006400050169640000000017ffffffff007374617475",
            "730000000019ffffffff00616d6f756e7400000006a4000a0006006665656c696e67000000400",
            "6ffffffff006e6f74650000000019ffffffff"
        ),
        streaming: false,
        expected: concat!(
            "RELATION      oid=16397 public.orders replident='d' cols=[KEY id(oid=23,mod=-1), ",
            "status(oid=25,mod=-1), amount(oid=1700,mod=655366), feeling(oid=16390,mod=-1), ",
            "note(oid=25,mod=-1)]"
        ),
    },
    Vector {
        name: "insert",
        hex: concat!(
            "490000400d4e000574000000013174000000036e6577740000000531392e3939740000000568617070",
            "7974000000056669727374"
        ),
        streaming: false,
        expected: "INSERT        rel=16397 new=['1', 'new', '19.99', 'happy', 'first']",
    },
    Vector {
        name: "update_pk_change",
        hex: concat!(
            "550000400d4b00057400000001316e6e6e6e4e00057400000003313030740000000773686970706564",
            "740000000532392e39397400000005686170707974000000056669727374"
        ),
        streaming: false,
        expected: concat!(
            "UPDATE        rel=16397 old[K]=['1', NULL, NULL, NULL, NULL] ",
            "new=['100', 'shipped', '29.99', 'happy', 'first']"
        ),
    },
    Vector {
        name: "delete_key",
        hex: "440000400d4b000574000000033130306e6e6e6e",
        streaming: false,
        expected: "DELETE        rel=16397 old[K]=['100', NULL, NULL, NULL, NULL]",
    },
    Vector {
        name: "commit",
        hex: "4300000000000199bac8000000000199baf80002f8dc466a6b4f",
        streaming: false,
        expected: concat!(
            "COMMIT        flags=0 commit_lsn=0/199BAC8 end_lsn=0/199BAF8 ",
            "commit_ts=2026-07-05T13:55:11.294287+00:00"
        ),
    },
    // ---- the REPLICA IDENTITY contrast ----
    Vector {
        name: "update_no_key_change",
        hex: "550000410b4e00057400000001357400000001626e6e74000000016e",
        streaming: false,
        expected: "UPDATE        rel=16651 new=['5', 'b', NULL, NULL, 'n']",
    },
    Vector {
        name: "update_full_identity",
        hex: concat!(
            "55000041194f00037400000001377400000001777400000001314e0003740000000137740000000177",
            "740000000132"
        ),
        streaming: false,
        expected: "UPDATE        rel=16665 old[O]=['7', 'w', '1'] new=['7', 'w', '2']",
    },
    Vector {
        name: "delete_full_identity",
        hex: "44000041194f0003740000000137740000000177740000000132",
        streaming: false,
        expected: "DELETE        rel=16665 old[O]=['7', 'w', '2']",
    },
    // ---- truncate option bits, logical messages ----
    Vector {
        name: "truncate_plain",
        hex: "5400000001000000410b",
        streaming: false,
        expected: "TRUNCATE      opts=none rels=[16651]",
    },
    Vector {
        name: "truncate_cascade_restart",
        hex: "54000000010300004119",
        streaming: false,
        expected: "TRUNCATE      opts=CASCADE|RESTART IDENTITY rels=[16665]",
    },
    Vector {
        name: "message_non_transactional",
        hex: "4d000000000001fb4be077616c727573000000000668622d6e6f6e",
        streaming: false,
        expected: "MESSAGE       non-transactional lsn=0/1FB4BE0 prefix='walrus' content=b'hb-non'",
    },
    Vector {
        name: "message_transactional",
        hex: "4d010000000001fb4ba077616c727573000000000668622d74786e",
        streaming: false,
        expected: "MESSAGE       transactional lsn=0/1FB4BA0 prefix='walrus' content=b'hb-txn'",
    },
    // ---- streaming frames (v2+) ----
    Vector {
        name: "stream_start_first",
        hex: "53000002f101",
        streaming: false,
        expected: "STREAM START  xid=753 first_segment=1",
    },
    Vector {
        name: "stream_stop",
        hex: "45",
        streaming: true,
        expected: "STREAM STOP",
    },
    Vector {
        name: "stream_commit",
        hex: "63000002f5000000000001e095800000000001e095b80002f8dc50e8dd86",
        streaming: true,
        expected: concat!(
            "STREAM COMMIT xid=757 flags=0 commit_lsn=0/1E09580 end_lsn=0/1E095B8 ",
            "commit_ts=2026-07-05T13:58:07.353222+00:00"
        ),
    },
    Vector {
        name: "stream_abort_subtransaction",
        hex: "41000002f5000002f6",
        streaming: true,
        expected: "STREAM ABORT  top_xid=757 sub_xid=758  <- SUBTRANSACTION rollback",
    },
    Vector {
        name: "stream_abort_whole_txn",
        hex: "410000036200000362",
        streaming: true,
        expected: "STREAM ABORT  top_xid=866 sub_xid=866  <- WHOLE-TXN abort",
    },
    Vector {
        name: "streamed_insert_carries_xid",
        hex: "49000002f10000410b4e00057400000001317400000001736e6e74000000027879",
        streaming: true,
        expected: "INSERT        xid=753 rel=16651 new=['1', 's', NULL, NULL, 'xy']",
    },
    Vector {
        name: "unchanged_toast_update",
        hex: "550000410b4e00057400000001397400000001796e6e75",
        streaming: false,
        expected: "UPDATE        rel=16651 new=['9', 'y', NULL, NULL, <unchanged-TOAST>]",
    },
    // ---- schema evolution / composite key / generated columns (captured) ----
    Vector {
        name: "relation_composite_pk",
        hex: concat!(
            "520000636e7075626c696300637573746f6d6572730064000301726567696f6e0000000019ffffffff",
            "0169640000000017ffffffff006e616d650000000019ffffffff"
        ),
        streaming: false,
        expected: concat!(
            "RELATION      oid=25454 public.customers replident='d' ",
            "cols=[KEY region(oid=25,mod=-1), KEY id(oid=23,mod=-1), name(oid=25,mod=-1)]"
        ),
    },
    Vector {
        name: "update_composite_pk_partial",
        hex: concat!(
            "550000636e4b0003740000000275737400000001316e4e000374000000026575740000000131740000",
            "000141"
        ),
        streaming: false,
        expected: "UPDATE        rel=25454 old[K]=['us', '1', NULL] new=['eu', '1', 'A']",
    },
    Vector {
        name: "relation_full_identity_all_keys",
        hex: concat!(
            "52000063757075626c6963006974656d73006600030169640000000017ffffffff016c6162656c0000",
            "000019ffffffff017174790000000017ffffffff"
        ),
        streaming: false,
        expected: concat!(
            "RELATION      oid=25461 public.items replident='f' ",
            "cols=[KEY id(oid=23,mod=-1), KEY label(oid=25,mod=-1), KEY qty(oid=23,mod=-1)]"
        ),
    },
    Vector {
        name: "insert_generated_column_omitted",
        hex: "49000063754e000374000000013274000000016774000000023130",
        streaming: false,
        expected: "INSERT        rel=25461 new=['2', 'g', '10']",
    },
    Vector {
        name: "relation_after_add_column",
        hex: concat!(
            "52000063677075626c6963006f7264657273006400060169640000000017ffffffff00737461747573",
            "0000000019ffffffff00616d6f756e7400000006a4000a0006006665656c696e670000006361ffffff",
            "ff006e6f74650000000019ffffffff0065787472610000000017ffffffff"
        ),
        streaming: false,
        expected: concat!(
            "RELATION      oid=25447 public.orders replident='d' cols=[KEY id(oid=23,mod=-1), ",
            "status(oid=25,mod=-1), amount(oid=1700,mod=655366), feeling(oid=25441,mod=-1), ",
            "note(oid=25,mod=-1), extra(oid=23,mod=-1)]"
        ),
    },
    // ---- two-phase commit messages (v3; CONSTRUCTED — walrus is v2-only, never sees these,
    //      but the decoder must parse them; xid=100, gid='gtx') ----
    Vector {
        name: "begin_prepare",
        hex: "62000000000100000000000000010001000002f8dc000000000000006467747800",
        streaming: false,
        expected: concat!(
            "BEGIN PREPARE prepare_lsn=0/1000000 end_lsn=0/1000100 ",
            "ts=2026-07-05T13:35:29.914880+00:00 xid=100 gid='gtx'"
        ),
    },
    Vector {
        name: "prepare",
        hex: "5000000000000100000000000000010001000002f8dc000000000000006467747800",
        streaming: false,
        expected: concat!(
            "PREPARE       prepare_lsn=0/1000000 end_lsn=0/1000100 ",
            "ts=2026-07-05T13:35:29.914880+00:00 xid=100 gid='gtx'"
        ),
    },
    Vector {
        name: "commit_prepared",
        hex: "4b00000000000100020000000000010003000002f8dc000000010000006467747800",
        streaming: false,
        expected: concat!(
            "COMMIT PREP   commit_lsn=0/1000200 end_lsn=0/1000300 ",
            "ts=2026-07-05T13:35:29.914881+00:00 xid=100 gid='gtx'"
        ),
    },
    Vector {
        name: "rollback_prepared",
        hex: concat!(
            "7200000000000100020000000000010004000002f8dc000000000002f8dc0000000200000064",
            "67747800"
        ),
        streaming: false,
        expected: concat!(
            "ROLLBACK PREP end_lsn=0/1000200 rollback_lsn=0/1000400 ",
            "prepare_ts=2026-07-05T13:35:29.914880+00:00 rollback_ts=2026-07-05T13:35:29.914882",
            "+00:00 xid=100 gid='gtx'"
        ),
    },
];

/// Test-only convenience: hex → one `Message`. (Reader/parse signature finalised in PR 2.2.)
pub fn decode(hex: &str, streaming: bool) -> Message {
    let bytes = hex::decode(hex).expect("vector hex is valid");
    let mut ctx = StreamCtx {
        in_stream: streaming,
    };
    parse_message(&bytes, &mut ctx)
}

/// Test-only renderer that reproduces `decode_pgoutput.py::render` line-for-line. Lives in the test
/// crate, NOT the lib — production stamps time as RFC-3339 `Z` (PR 1.1); this helper must match
/// Python's `+00:00`/`X/Y`-hex output to compare against `expected`. Filled in PR 2.2.
pub fn render(_m: &Message) -> String {
    todo!("PR 2.2 — implement alongside the first decoded messages")
}

#[test]
#[ignore = "turned on family-by-family in PRs 2.2–2.8"]
fn all_vectors_present_and_hex_decodable() {
    for v in VECTORS {
        assert!(hex::decode(v.hex).is_ok(), "vector {} has bad hex", v.name);
    }
    // Assert the full count so a dropped/renamed vector fails loudly.
    assert_eq!(VECTORS.len(), 30, "expected the full ported VECTORS set");
}
