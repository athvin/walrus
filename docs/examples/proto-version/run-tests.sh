#!/usr/bin/env bash
# run-tests.sh — live-Postgres ASSERTION suite (pass/fail) for the behaviors that can't be a
# static golden vector: streaming triggers, savepoint aborts, interleaving, slot mechanics.
# The decoder-level parsing is covered by test_decode_pgoutput.py; this covers the wire behavior.
#
#   docker compose up --wait
#   docker exec -i walrus-proto-pg psql -U postgres -d walrus < 01-setup.sql
#   ./run-tests.sh
#
# Cases are drawn from the adversarially-verified edge-case catalog (docs TESTING.md).
set -uo pipefail
CID="${CID:-walrus-proto-pg}"
HERE="$(cd "$(dirname "$0")" && pwd)"
PASS=0; FAIL=0
psql(){ docker exec -i "$CID" psql -U postgres -d walrus "$@"; }
q(){ psql -At -c "$1"; }
run(){ psql -q -c "$1" >/dev/null 2>&1; }
dec(){ python3 "$HERE/decode_pgoutput.py"; }
# peek pgoutput (streaming on) -> decoded lines
peek(){ q "SELECT encode(data,'hex') FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','on','messages','on')" | dec; }
peek_hex(){ q "SELECT encode(data,'hex') FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','on')"; }
reset(){ q "TRUNCATE orders, items, customers;" >/dev/null
  q "SELECT count(*) FROM pg_logical_slot_get_changes('slot_test',NULL,NULL,'stream-changes','1')" >/dev/null
  q "SELECT count(*) FROM pg_logical_slot_get_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','on')" >/dev/null; }
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$1"; }
no(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s  (got: %s)\n' "$1" "$2"; }
eq(){ [ "$2" = "$3" ] && ok "$1" || no "$1 [want $2]" "$3"; }
ge(){ [ "$2" -ge "$3" ] 2>/dev/null && ok "$1" || no "$1 [want >= $3]" "$2"; }
hdr(){ printf '\n\033[1m%s\033[0m\n' "$1"; }

# ---------------------------------------------------------------------------
hdr "1. subtransaction rollback: 1 Stream Abort (sub!=top) + 1 Stream Commit; aborted rows dropped"
reset
run "BEGIN;
  INSERT INTO orders(id,status,note) SELECT 20000+g,'A',repeat('a',40) FROM generate_series(1,2000) g;
  SAVEPOINT sp; INSERT INTO orders(id,status,note) SELECT 30000+g,'RB',repeat('b',40) FROM generate_series(1,2000) g; ROLLBACK TO sp;
  INSERT INTO orders(id,status,note) SELECT 40000+g,'B',repeat('c',40) FROM generate_series(1,2000) g;
COMMIT;"
d=$(peek)
eq "one Stream Abort frame" 1 "$(printf '%s\n' "$d" | grep -c 'STREAM ABORT')"
eq "it is a SUBTRANSACTION rollback" 1 "$(printf '%s\n' "$d" | grep -c 'SUBTRANSACTION rollback')"
eq "one Stream Commit frame" 1 "$(printf '%s\n' "$d" | grep -c 'STREAM COMMIT')"
naborts=$(peek_hex | grep -c '^41'); eq "exactly one abort on the wire" 1 "$naborts"
# 3 distinct per-message xids on inserts; the source keeps only 4000 rows (A+B)
nxids=$(peek_hex | grep '^49' | python3 -c "import sys;print(len({int.from_bytes(bytes.fromhex(l.strip())[1:5],'big') for l in sys.stdin if l.strip()}))")
eq "3 distinct (sub)xids among streamed inserts" 3 "$nxids"
eq "source table kept only A+B rows" 4000 "$(q "SELECT count(*) FROM orders WHERE id>=20000")"

hdr "2. NESTED savepoints, one ROLLBACK TO: TWO Stream Aborts (same top), innermost-first"
reset
run "BEGIN;
  INSERT INTO orders(id,status,note) SELECT 100000+g,'T',repeat('t',40) FROM generate_series(1,2000) g;
  SAVEPOINT sp1; INSERT INTO orders(id,status,note) SELECT 200000+g,'O',repeat('a',40) FROM generate_series(1,2000) g;
    SAVEPOINT sp2; INSERT INTO orders(id,status,note) SELECT 300000+g,'I',repeat('b',40) FROM generate_series(1,2000) g;
  ROLLBACK TO sp1;
  INSERT INTO orders(id,status,note) SELECT 400000+g,'AF',repeat('c',40) FROM generate_series(1,2000) g;
COMMIT;"
d=$(peek)
eq "two Stream Abort frames" 2 "$(printf '%s\n' "$d" | grep -c 'STREAM ABORT')"
eq "one Stream Commit" 1 "$(printf '%s\n' "$d" | grep -c 'STREAM COMMIT')"
# the two aborts share the same top_xid
tops=$(printf '%s\n' "$d" | grep 'STREAM ABORT' | grep -oE 'top_xid=[0-9]+' | sort -u | wc -l | tr -d ' ')
eq "both aborts share one top_xid" 1 "$tops"
# innermost-first => sub_xids arrive in DESCENDING order
subs=$(printf '%s\n' "$d" | grep 'STREAM ABORT' | grep -oE 'sub_xid=[0-9]+' | grep -oE '[0-9]+' | tr '\n' ' ')
first=$(echo $subs | awk '{print $1}'); second=$(echo $subs | awk '{print $2}')
[ "$first" -gt "$second" ] 2>/dev/null && ok "sub_xids descending (innermost first): $subs" || no "sub_xids descending" "$subs"
eq "source kept T+AF only" 4000 "$(q "SELECT count(*) FROM orders WHERE id>=100000")"

hdr "3. RELEASE SAVEPOINT: rows KEPT, ZERO Stream Abort (sub_xid!=top with no abort => keep)"
reset
run "BEGIN;
  INSERT INTO orders(id,status,note) SELECT 100000+g,'T',repeat('t',40) FROM generate_series(1,2000) g;
  SAVEPOINT sp; INSERT INTO orders(id,status,note) SELECT 200000+g,'REL',repeat('a',40) FROM generate_series(1,2000) g; RELEASE SAVEPOINT sp;
  INSERT INTO orders(id,status,note) SELECT 300000+g,'AF',repeat('c',40) FROM generate_series(1,2000) g;
COMMIT;"
d=$(peek)
eq "zero Stream Abort frames" 0 "$(printf '%s\n' "$d" | grep -c 'STREAM ABORT')"
eq "one Stream Commit" 1 "$(printf '%s\n' "$d" | grep -c 'STREAM COMMIT')"
eq "all 6000 rows survive" 6000 "$(q "SELECT count(*) FROM orders WHERE id>=100000")"

hdr "4. small ABORTED txn (below threshold): NOTHING emitted via the SQL path"
reset
run "BEGIN; INSERT INTO orders(id,status) SELECT g,'x' FROM generate_series(1,5) g; ROLLBACK;"
eq "no messages for a small rolled-back txn" 0 "$(peek_hex | grep -c .)"

hdr "5. same-commit TRUNCATE; INSERT: share ONE commit_lsn (tuple-boundary hazard)"
reset
run "INSERT INTO orders(id,status) VALUES (1,'old');"; reset
run "BEGIN; TRUNCATE orders; INSERT INTO orders(id,status) VALUES (2,'postT'); COMMIT;"
d=$(peek)
clsn=$(printf '%s\n' "$d" | grep '^COMMIT' | grep -oE 'commit_lsn=[0-9A-F/]+' | head -1)
eq "has a TRUNCATE" 1 "$(printf '%s\n' "$d" | grep -c '^TRUNCATE')"
eq "has the post-truncate INSERT" 1 "$(printf '%s\n' "$d" | grep -c "^INSERT")"
# both are inside ONE txn (one Begin, one Commit) => they share the commit_lsn
eq "single transaction (one Begin)" 1 "$(printf '%s\n' "$d" | grep -c '^BEGIN')"
[ -n "$clsn" ] && ok "post-truncate insert shares the txn commit_lsn ($clsn)" || no "commit_lsn present" "$clsn"

hdr "6. empty transactions are fully suppressed (pgoutput skips empty xacts)"
reset
run "BEGIN; COMMIT;"
run "BEGIN; SELECT 1; COMMIT;"   # no data change -> no Begin/Commit on the wire
eq "no Begin/Commit/Insert for empty transactions" 0 "$(peek_hex | grep -c '^\(42\|43\|49\)')"

hdr "7. two concurrent large txns: interleave; commit order may differ from start order"
reset
q "BEGIN; INSERT INTO orders(id,status,note) SELECT 500000+g,'A',repeat('A',30) FROM generate_series(1,6000) g; COMMIT;" >/dev/null &
q "BEGIN; INSERT INTO orders(id,status,note) SELECT 600000+g,'B',repeat('B',30) FROM generate_series(1,6000) g; COMMIT;" >/dev/null &
wait
d=$(peek)
eq "two Stream Commits (both large txns streamed)" 2 "$(printf '%s\n' "$d" | grep -c 'STREAM COMMIT')"
# at least one interleave: the Stream Start xids are not a single monotonic run
nstart_xids=$(printf '%s\n' "$d" | grep 'STREAM START' | grep -oE 'xid=[0-9]+' | uniq | wc -l | tr -d ' ')
ge "stream-start xids alternate (interleaved blocks)" "$nstart_xids" 3

hdr "8. protocol guards"
reset
err=$(psql -c "SELECT 1 FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,'proto_version','1','publication_names','pub','streaming','on')" 2>&1 | grep -c 'does not support streaming')
eq "v1 + streaming rejected" 1 "$err"

hdr "9. peek is non-consuming, get is consuming (idempotency)"
reset
run "INSERT INTO orders(id,status) VALUES (7,'a');"
a=$(peek_hex | grep -c .); b=$(peek_hex | grep -c .)
eq "two peeks return the same rows" "$a" "$b"
q "SELECT count(*) FROM pg_logical_slot_get_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub')" >/dev/null
eq "after get, peek is empty (slot advanced)" 0 "$(peek_hex | grep -c .)"

hdr "10. idle-publication heartbeat: a committed write on the slot advances confirmed_flush_lsn"
# The §1.9 heartbeat: when published tables are idle, a periodic committed write (a published
# heartbeat table, here stood in for by a committed row) gives the slot a fresh commit LSN to
# confirm past, so restart_lsn/confirmed_flush_lsn keep advancing and WAL cannot pin.
reset
before=$(q "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='slot_pg'")
run "INSERT INTO orders(id,status) VALUES (999,'heartbeat');"    # a committed change -> commit boundary
q "SELECT count(*) FROM pg_logical_slot_get_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub')" >/dev/null
after=$(q "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='slot_pg'")
[ "$(q "SELECT '$after'::pg_lsn > '$before'::pg_lsn")" = "t" ] && ok "confirmed_flush_lsn advanced ($before -> $after)" || no "slot advanced" "$before -> $after"

hdr "11. two slots are independent (consuming one does not affect the other)"
reset
run "INSERT INTO orders(id,status) VALUES (8,'a');"
q "SELECT count(*) FROM pg_logical_slot_get_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub')" >/dev/null
eq "slot_test still sees the change after slot_pg consumed it" 1 \
   "$(q "SELECT count(*) FROM pg_logical_slot_peek_changes('slot_test',NULL,NULL) WHERE data LIKE '%INSERT%'")"

hdr "12. REPLICA IDENTITY NOTHING: publisher refuses UPDATE/DELETE"
reset
run "ALTER TABLE orders REPLICA IDENTITY NOTHING; INSERT INTO orders(id,status) VALUES (9,'a');"
err=$(psql -c "UPDATE orders SET status='b' WHERE id=9" 2>&1 | grep -ic 'cannot update')
run "ALTER TABLE orders REPLICA IDENTITY DEFAULT;"   # restore
eq "UPDATE rejected under REPLICA IDENTITY NOTHING" 1 "$err"

printf '\n\033[1m==== %d passed, %d failed ====\033[0m\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
