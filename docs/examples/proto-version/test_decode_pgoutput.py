#!/usr/bin/env python3
"""
Golden-vector unit tests for decode_pgoutput.py.

    python3 -m unittest test_decode_pgoutput -v     # or:  python3 test_decode_pgoutput.py

These are PURE tests — no Docker, no Postgres. Each vector is a real (or, where noted,
minimally hand-built and cross-checked) pgoutput message in hex; we assert the decoder parses
it to the right structure and renders the right line. They double as **portable fixtures for
the future Rust sink decoder**: the same {bytes -> expected fields} pairs should hold there.

The flagship is `Semantics.test_subtransaction_abort_*` — the rolled-back-savepoint case that
silently corrupts a mirror if the sub-transaction xid on Stream Abort is ignored
(see docs/proto-version.md §9b).

Note: OIDs/xids differ between capture runs, so vectors captured at different times carry
different ids — each vector is self-contained.
"""
import unittest

import decode_pgoutput as d

# name -> (hex, streaming_context, expected_rendered_line)
# "streaming_context" = were we inside a Stream Start..Stop block when this message arrived
# (only then does a per-message xid prefix exist).
VECTORS = {
    # ---- non-streamed transaction control + DML (REPLICA IDENTITY DEFAULT) ----
    "begin":
        ("42000000000199bac80002f8dc466a6b4f000002ed", False,
         "BEGIN         final_lsn=0/199BAC8 commit_ts=2026-07-05T13:55:11.294287+00:00 xid=749"),
    "type_enum":
        ("59000040067075626c6963006d6f6f6400", False,
         "TYPE          oid=16390 public.mood"),
    "relation_orders":
        ("520000400d7075626c6963006f7264657273006400050169640000000017ffffffff007374617475"
         "730000000019ffffffff00616d6f756e7400000006a4000a0006006665656c696e67000000400"
         "6ffffffff006e6f74650000000019ffffffff", False,
         "RELATION      oid=16397 public.orders replident='d' cols=[KEY id(oid=23,mod=-1), "
         "status(oid=25,mod=-1), amount(oid=1700,mod=655366), feeling(oid=16390,mod=-1), "
         "note(oid=25,mod=-1)]"),
    "insert":
        ("490000400d4e000574000000013174000000036e6577740000000531392e3939740000000568617070"
         "7974000000056669727374", False,
         "INSERT        rel=16397 new=['1', 'new', '19.99', 'happy', 'first']"),
    "update_pk_change":  # PK changed -> old KEY image ('K'), key-only (rest NULL)
        ("550000400d4b00057400000001316e6e6e6e4e00057400000003313030740000000773686970706564"
         "740000000532392e39397400000005686170707974000000056669727374", False,
         "UPDATE        rel=16397 old[K]=['1', NULL, NULL, NULL, NULL] "
         "new=['100', 'shipped', '29.99', 'happy', 'first']"),
    "delete_key":
        ("440000400d4b000574000000033130306e6e6e6e", False,
         "DELETE        rel=16397 old[K]=['100', NULL, NULL, NULL, NULL]"),
    "commit":
        ("4300000000000199bac8000000000199baf80002f8dc466a6b4f", False,
         "COMMIT        flags=0 commit_lsn=0/199BAC8 end_lsn=0/199BAF8 "
         "commit_ts=2026-07-05T13:55:11.294287+00:00"),

    # ---- the REPLICA IDENTITY contrast ----
    "update_no_key_change":  # non-key UPDATE, DEFAULT identity -> NO old image at all
        ("550000410b4e00057400000001357400000001626e6e74000000016e", False,
         "UPDATE        rel=16651 new=['5', 'b', NULL, NULL, 'n']"),
    "update_full_identity":  # REPLICA IDENTITY FULL -> whole old row ('O')
        ("55000041194f00037400000001377400000001777400000001314e0003740000000137740000000177"
         "740000000132", False,
         "UPDATE        rel=16665 old[O]=['7', 'w', '1'] new=['7', 'w', '2']"),
    "delete_full_identity":
        ("44000041194f0003740000000137740000000177740000000132", False,
         "DELETE        rel=16665 old[O]=['7', 'w', '2']"),

    # ---- truncate option bits, logical messages ----
    "truncate_plain":
        ("5400000001000000410b", False,
         "TRUNCATE      opts=none rels=[16651]"),
    "truncate_cascade_restart":
        ("54000000010300004119", False,
         "TRUNCATE      opts=CASCADE|RESTART IDENTITY rels=[16665]"),
    "message_non_transactional":
        ("4d000000000001fb4be077616c727573000000000668622d6e6f6e", False,
         "MESSAGE       non-transactional lsn=0/1FB4BE0 prefix='walrus' content=b'hb-non'"),
    "message_transactional":
        ("4d010000000001fb4ba077616c727573000000000668622d74786e", False,
         "MESSAGE       transactional lsn=0/1FB4BA0 prefix='walrus' content=b'hb-txn'"),

    # ---- streaming frames (v2+) ----
    "stream_start_first":
        ("53000002f101", False, "STREAM START  xid=753 first_segment=1"),
    "stream_stop":
        ("45", True, "STREAM STOP"),
    "stream_commit":
        ("63000002f5000000000001e095800000000001e095b80002f8dc50e8dd86", True,
         "STREAM COMMIT xid=757 flags=0 commit_lsn=0/1E09580 end_lsn=0/1E095B8 "
         "commit_ts=2026-07-05T13:58:07.353222+00:00"),
    # sub_xid (758) != top_xid (757): a rolled-back SAVEPOINT inside a committing txn.
    "stream_abort_subtransaction":
        ("41000002f5000002f6", True,
         "STREAM ABORT  top_xid=757 sub_xid=758  <- SUBTRANSACTION rollback"),
    # sub_xid == top_xid: the whole transaction aborted (constructed; matches live capture).
    "stream_abort_whole_txn":
        ("410000036200000362", True,
         "STREAM ABORT  top_xid=866 sub_xid=866  <- WHOLE-TXN abort"),
    # a change INSIDE a streamed block carries the per-message (sub)xact xid (constructed).
    "streamed_insert_carries_xid":
        ("49000002f10000410b4e00057400000001317400000001736e6e740000000278 79".replace(" ", ""),
         True,
         "INSERT        xid=753 rel=16651 new=['1', 's', NULL, NULL, 'xy']"),
    # unchanged-TOAST placeholder: the value is NOT on the wire (constructed).
    "unchanged_toast_update":
        ("550000410b4e0005740000000139740000000179" "6e6e75", False,
         "UPDATE        rel=16651 new=['9', 'y', NULL, NULL, <unchanged-TOAST>]"),

    # ---- schema evolution / composite key / generated columns (captured) ----
    "relation_composite_pk":  # two KEY columns
        ("520000636e7075626c696300637573746f6d6572730064000301726567696f6e0000000019ffffffff"
         "0169640000000017ffffffff006e616d650000000019ffffffff", False,
         "RELATION      oid=25454 public.customers replident='d' "
         "cols=[KEY region(oid=25,mod=-1), KEY id(oid=23,mod=-1), name(oid=25,mod=-1)]"),
    "update_composite_pk_partial":  # only region changed, old KEY still carries BOTH key cols
        ("550000636e4b0003740000000275737400000001316e4e000374000000026575740000000131740000"
         "000141", False,
         "UPDATE        rel=25454 old[K]=['us', '1', NULL] new=['eu', '1', 'A']"),
    "relation_full_identity_all_keys":  # REPLICA IDENTITY FULL -> every column flagged KEY
        ("52000063757075626c6963006974656d73006600030169640000000017ffffffff016c6162656c0000"
         "000019ffffffff017174790000000017ffffffff", False,
         "RELATION      oid=25461 public.items replident='f' "
         "cols=[KEY id(oid=23,mod=-1), KEY label(oid=25,mod=-1), KEY qty(oid=23,mod=-1)]"),
    "insert_generated_column_omitted":  # a GENERATED ALWAYS STORED col is NOT replicated
        ("49000063754e000374000000013274000000016774000000023130", False,
         "INSERT        rel=25461 new=['2', 'g', '10']"),
    "relation_after_add_column":  # same OID, now 6 cols incl. 'extra'
        ("52000063677075626c6963006f7264657273006400060169640000000017ffffffff00737461747573"
         "0000000019ffffffff00616d6f756e7400000006a4000a0006006665656c696e670000006361ffffff"
         "ff006e6f74650000000019ffffffff0065787472610000000017ffffffff", False,
         "RELATION      oid=25447 public.orders replident='d' cols=[KEY id(oid=23,mod=-1), "
         "status(oid=25,mod=-1), amount(oid=1700,mod=655366), feeling(oid=25441,mod=-1), "
         "note(oid=25,mod=-1), extra(oid=23,mod=-1)]"),

    # ---- two-phase commit messages (v3; CONSTRUCTED — walrus is v2-only, never sees these,
    #      but the decoder must parse them; xid=100, gid='gtx') ----
    "begin_prepare":
        ("62000000000100000000000000010001000002f8dc000000000000006467747800", False,
         "BEGIN PREPARE prepare_lsn=0/1000000 end_lsn=0/1000100 "
         "ts=2026-07-05T13:35:29.914880+00:00 xid=100 gid='gtx'"),
    "prepare":
        ("5000000000000100000000000000010001000002f8dc000000000000006467747800", False,
         "PREPARE       prepare_lsn=0/1000000 end_lsn=0/1000100 "
         "ts=2026-07-05T13:35:29.914880+00:00 xid=100 gid='gtx'"),
    "commit_prepared":
        ("4b00000000000100020000000000010003000002f8dc000000010000006467747800", False,
         "COMMIT PREP   commit_lsn=0/1000200 end_lsn=0/1000300 "
         "ts=2026-07-05T13:35:29.914881+00:00 xid=100 gid='gtx'"),
    "rollback_prepared":
        ("7200000000000100020000000000010004000002f8dc000000000002f8dc0000000200000064"
         "67747800", False,
         "ROLLBACK PREP end_lsn=0/1000200 rollback_lsn=0/1000400 "
         "prepare_ts=2026-07-05T13:35:29.914880+00:00 rollback_ts=2026-07-05T13:35:29.914882"
         "+00:00 xid=100 gid='gtx'"),
}


class GoldenRender(unittest.TestCase):
    """Every vector must render to its exact golden line (the human-verified ground truth)."""
    def test_render(self):
        for name, (hexs, streaming, expected) in VECTORS.items():
            with self.subTest(vector=name):
                self.assertEqual(d.render(d.parse_hex(hexs, streaming)), expected)

    def test_render_is_stable(self):
        # parse -> render must be deterministic
        for name, (hexs, streaming, _) in VECTORS.items():
            with self.subTest(vector=name):
                a = d.render(d.parse_hex(hexs, streaming))
                b = d.render(d.parse_hex(hexs, streaming))
                self.assertEqual(a, b)


class Semantics(unittest.TestCase):
    """The parsed structure must encode the walrus-relevant semantics correctly."""

    # -- the flagship: subtransaction rollback must be distinguishable and NOT a whole-txn abort
    def test_subtransaction_abort_is_not_whole_txn(self):
        m = d.parse_hex(VECTORS["stream_abort_subtransaction"][0], streaming=True)
        self.assertEqual(m["type"], "stream_abort")
        self.assertNotEqual(m["top_xid"], m["sub_xid"])   # 757 != 758
        self.assertFalse(m["whole_txn"])                   # => discard only sub_xid's rows,
                                                           #    the top-level txn still commits
        self.assertEqual((m["top_xid"], m["sub_xid"]), (757, 758))

    def test_whole_txn_abort_flagged(self):
        m = d.parse_hex(VECTORS["stream_abort_whole_txn"][0], streaming=True)
        self.assertEqual(m["top_xid"], m["sub_xid"])
        self.assertTrue(m["whole_txn"])                    # => discard the entire transaction

    def test_default_identity_update_without_key_change_has_no_old_image(self):
        m = d.parse_hex(VECTORS["update_no_key_change"][0])
        self.assertEqual(m["type"], "update")
        self.assertIsNone(m["old_kind"])                   # no 'K' and no 'O'
        self.assertIsNone(m["old"])

    def test_pk_changing_update_carries_key_only_old_image(self):
        m = d.parse_hex(VECTORS["update_pk_change"][0])
        self.assertEqual(m["old_kind"], "K")
        # key column present, the rest NULL (DEFAULT identity ships key columns only)
        self.assertEqual(m["old"][0], {"fmt": "t", "value": "1"})
        self.assertTrue(all(c["fmt"] == "n" for c in m["old"][1:]))

    def test_full_identity_carries_whole_old_row(self):
        m = d.parse_hex(VECTORS["update_full_identity"][0])
        self.assertEqual(m["old_kind"], "O")
        self.assertEqual([c.get("value") for c in m["old"]], ["7", "w", "1"])  # every column

    def test_unchanged_toast_value_absent_from_wire(self):
        m = d.parse_hex(VECTORS["unchanged_toast_update"][0])
        self.assertEqual(m["new"][4], {"fmt": "u"})        # 'u' => value NOT sent; recover it downstream
        self.assertNotIn("value", m["new"][4])

    def test_null_vs_toast_are_distinct(self):
        m = d.parse_hex(VECTORS["unchanged_toast_update"][0])
        self.assertEqual(m["new"][2]["fmt"], "n")          # a real NULL
        self.assertEqual(m["new"][4]["fmt"], "u")          # unchanged-TOAST — different thing
        self.assertNotEqual(m["new"][2], m["new"][4])

    def test_truncate_option_bits(self):
        plain = d.parse_hex(VECTORS["truncate_plain"][0])
        self.assertEqual((plain["cascade"], plain["restart_identity"]), (False, False))
        both = d.parse_hex(VECTORS["truncate_cascade_restart"][0])
        self.assertEqual((both["cascade"], both["restart_identity"]), (True, True))

    def test_message_transactional_flag(self):
        self.assertFalse(d.parse_hex(VECTORS["message_non_transactional"][0])["transactional"])
        self.assertTrue(d.parse_hex(VECTORS["message_transactional"][0])["transactional"])

    def test_relation_marks_key_columns(self):
        m = d.parse_hex(VECTORS["relation_orders"][0])
        keys = [c["name"] for c in m["columns"] if c["key"]]
        self.assertEqual(keys, ["id"])                     # only the PK column is flagged key
        self.assertEqual(m["replident"], "d")              # DEFAULT identity


class XidPrefixOnlyWhenStreaming(unittest.TestCase):
    """The per-message xid exists ONLY inside a streamed block — this is the whole reason the
    consumer can demultiplex interleaved transactions (docs/proto-version.md §7)."""

    def test_same_bytes_parse_differently_in_and_out_of_stream(self):
        # a streamed INSERT: with streaming context, the xid prefix is consumed
        streamed = d.parse_hex(VECTORS["streamed_insert_carries_xid"][0], streaming=True)
        self.assertEqual(streamed["xid"], 753)
        self.assertEqual(streamed["relation_oid"], 16651)

    def test_non_streamed_change_has_no_xid(self):
        m = d.parse_hex(VECTORS["insert"][0], streaming=False)
        self.assertIsNone(m["xid"])

    def test_stream_start_stop_toggle_context(self):
        # Feed Start -> Insert(with xid) -> Stop -> and a following change has no prefix.
        ctx = [False]
        start = d.parse_message(d.Reader(bytes.fromhex("53000002f101")), ctx)
        self.assertEqual(start["type"], "stream_start")
        self.assertTrue(ctx[0])                            # Start opened the streamed context
        ins = d.parse_message(
            d.Reader(bytes.fromhex(VECTORS["streamed_insert_carries_xid"][0])), ctx)
        self.assertEqual(ins["xid"], 753)                  # prefix present while streaming
        d.parse_message(d.Reader(b"E"), ctx)               # Stream Stop
        self.assertFalse(ctx[0])                           # context closed


class SchemaAndKeys(unittest.TestCase):
    """Relation/tuple shapes under composite keys, FULL identity, generated columns, DDL."""

    def test_composite_pk_old_image_carries_all_key_columns(self):
        # only `region` changed, but the old KEY image still ships BOTH key columns
        # (region + id) so the consumer can locate the row; non-key `name` is NULL.
        m = d.parse_hex(VECTORS["update_composite_pk_partial"][0])
        self.assertEqual(m["old_kind"], "K")
        self.assertEqual(m["old"][0], {"fmt": "t", "value": "us"})   # region (key)
        self.assertEqual(m["old"][1], {"fmt": "t", "value": "1"})    # id (key)
        self.assertEqual(m["old"][2], {"fmt": "n"})                  # name (non-key) NULL

    def test_full_identity_relation_flags_every_column_key(self):
        m = d.parse_hex(VECTORS["relation_full_identity_all_keys"][0])
        self.assertEqual(m["replident"], "f")
        self.assertTrue(all(c["key"] for c in m["columns"]))         # ALL columns are key

    def test_generated_stored_column_is_omitted(self):
        # `items` has a GENERATED ALWAYS AS ... STORED column `dbl`; it appears in neither
        # the Relation nor the tuple — generated columns are not replicated.
        rel = d.parse_hex(VECTORS["relation_full_identity_all_keys"][0])
        self.assertNotIn("dbl", [c["name"] for c in rel["columns"]])
        ins = d.parse_hex(VECTORS["insert_generated_column_omitted"][0])
        self.assertEqual(len(ins["new"]), 3)                         # id, label, qty — no dbl

    def test_add_column_relation_reemitted_with_new_column(self):
        m = d.parse_hex(VECTORS["relation_after_add_column"][0])
        self.assertEqual([c["name"] for c in m["columns"]],
                         ["id", "status", "amount", "feeling", "note", "extra"])

    def test_numeric_typmod_encodes_precision_scale(self):
        # numeric(10,2) atttypmod = ((10<<16)|2)+4 = 655366 (survives; not the -1 sentinel)
        m = d.parse_hex(VECTORS["relation_orders"][0])
        amount = next(c for c in m["columns"] if c["name"] == "amount")
        self.assertEqual((amount["type_oid"], amount["typmod"]), (1700, 655366))


class TwoPhase(unittest.TestCase):
    """v3 two-phase messages. walrus runs at v2 and never enables two_phase, so it never sees
    these — but the decoder must still parse them without misalignment."""

    def test_2pc_messages_parse_with_gid_and_xid(self):
        for name, typ in [("begin_prepare", "begin_prepare"), ("prepare", "prepare"),
                          ("commit_prepared", "commit_prepared"),
                          ("rollback_prepared", "rollback_prepared")]:
            with self.subTest(msg=name):
                m = d.parse_hex(VECTORS[name][0])
                self.assertEqual(m["type"], typ)
                self.assertEqual(m["xid"], 100)
                self.assertEqual(m["gid"], "gtx")

    def test_commit_prepared_K_byte_disambiguated_from_update_key_marker(self):
        # The byte 'K' (0x4b) is BOTH the top-level Commit Prepared message id AND the
        # Update/Delete old-KEY submessage marker. Context resolves it: top-level parse vs
        # inside a tuple. Both must decode correctly.
        top = d.parse_hex(VECTORS["commit_prepared"][0])
        self.assertEqual(top["type"], "commit_prepared")            # 'K' as a message
        upd = d.parse_hex(VECTORS["update_pk_change"][0])
        self.assertEqual(upd["old_kind"], "K")                      # 'K' as a submessage marker


class StreamFraming(unittest.TestCase):
    """The live pg_recvlogical byte stream separates messages with 0x0a; parse_stream must
    skip those and decode a clean concatenation of self-delimiting messages."""

    def test_parse_stream_skips_newline_separators(self):
        # BEGIN \n COMMIT \n  (the 0x0a bytes are pg_recvlogical's separators, not data)
        begin = VECTORS["begin"][0]
        commit = VECTORS["commit"][0]
        raw = bytes.fromhex(begin) + b"\x0a" + bytes.fromhex(commit) + b"\x0a"
        msgs = d.parse_stream(raw)
        self.assertEqual([m["type"] for m in msgs], ["begin", "commit"])
        self.assertEqual(msgs[0]["xid"], 749)

    def test_parse_stream_preserves_streaming_context_across_messages(self):
        # Stream Start opens context; the following streamed INSERT keeps its xid prefix.
        raw = (bytes.fromhex("53000002f101") + b"\x0a"
               + bytes.fromhex(VECTORS["streamed_insert_carries_xid"][0]) + b"\x0a"
               + b"E" + b"\x0a")
        msgs = d.parse_stream(raw)
        self.assertEqual([m["type"] for m in msgs], ["stream_start", "insert", "stream_stop"])
        self.assertEqual(msgs[1]["xid"], 753)


if __name__ == "__main__":
    unittest.main(verbosity=2)
