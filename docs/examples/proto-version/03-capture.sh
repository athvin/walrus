#!/usr/bin/env bash
# 03-capture.sh — reproduce every capture in docs/proto-version.md against the live container.
#
#   docker compose up --wait
#   docker exec -i walrus-proto-pg psql -U postgres -d walrus < 01-setup.sql
#   ./03-capture.sh
#
# Each scenario resets both slots, runs the SQL, then shows the SAME change through
# test_decoding (readable) and pgoutput (decoded by decode_pgoutput.py). Uses peek where
# possible; consumes only to reset between scenarios, so the whole script is idempotent.
set -euo pipefail
CID="${CID:-walrus-proto-pg}"
HERE="$(cd "$(dirname "$0")" && pwd)"

psql()  { docker exec -i "$CID" psql -U postgres -d walrus "$@"; }
q()     { psql -At -c "$1"; }                       # quiet one-liner
decode(){ python3 "$HERE/decode_pgoutput.py"; }             # hex-lines mode (SQL path)
decode_stream(){ python3 "$HERE/decode_pgoutput.py" --stream; }  # raw pg_recvlogical byte stream

# empty the tables (so id ranges never collide across scenarios / re-runs) and consume
# everything on both slots — the TRUNCATE WAL is consumed here, so it is NOT captured.
reset_slots() {
  q "TRUNCATE orders, items, customers;" >/dev/null
  q "SELECT count(*) FROM pg_logical_slot_get_changes('slot_test',NULL,NULL,'stream-changes','1');" >/dev/null
  q "SELECT count(*) FROM pg_logical_slot_get_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','on');" >/dev/null
}
td()  { q "SELECT data FROM pg_logical_slot_peek_changes('slot_test',NULL,NULL,'include-xids','1','stream-changes','1');"; }
pgo() { q "SELECT encode(data,'hex') FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','on','messages','on');" | decode; }
hist(){ q "SELECT left(encode(data,'hex'),2) b, count(*) FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','on') GROUP BY 1 ORDER BY 2 DESC;"; }
hdr() { printf '\n\033[1m############ %s ############\033[0m\n' "$1"; }

# ---------------------------------------------------------------------------
hdr "1. BASIC DML (single-col PK, REPLICA IDENTITY DEFAULT)"
reset_slots
q "INSERT INTO orders(id,status,amount,feeling,note) VALUES (1,'new',19.99,'happy','first');
   UPDATE orders SET status='shipped',amount=29.99 WHERE id=1;
   UPDATE orders SET id=100 WHERE id=1;              -- PK-changing update -> 'K' old-key
   DELETE FROM orders WHERE id=100;" >/dev/null
echo '--- test_decoding ---'; td
echo '--- pgoutput (decoded) ---'; pgo

hdr "2. REPLICA IDENTITY FULL (whole old row via 'O'), composite PK, TRUNCATE, MESSAGE"
reset_slots
q "INSERT INTO items(id,label,qty) VALUES (1,'widget',5);
   UPDATE items SET qty=9 WHERE id=1;                -- FULL -> full old row
   DELETE FROM items WHERE id=1;
   INSERT INTO customers(region,id,name) VALUES ('us',1,'Acme');   -- composite PK
   TRUNCATE orders;
   TRUNCATE items RESTART IDENTITY CASCADE;
   SELECT pg_logical_emit_message(true ,'walrus','heartbeat-txn');
   SELECT pg_logical_emit_message(false,'walrus','heartbeat-nontxn');" >/dev/null
echo '--- test_decoding ---'; td
echo '--- pgoutput (decoded) ---'; pgo

hdr "3. UNCHANGED-TOAST ('u' — value NOT sent on the wire)"
reset_slots
q "ALTER TABLE orders ALTER COLUMN note SET STORAGE EXTERNAL;
   INSERT INTO orders(id,status,note) VALUES (888,'new',repeat('T',5000));
   UPDATE orders SET status='changed' WHERE id=888;" >/dev/null   # note unchanged
echo '--- test_decoding (UPDATE shows note[text]:u) ---'; td | grep -i update || true
echo '--- pgoutput (decoded; UPDATE new tuple = <unchanged-TOAST>) ---'; pgo | grep -E 'INSERT|UPDATE'

hdr "4. LARGE COMMITTED TXN -> streamed in blocks (Stream Start/Stop/Commit)"
reset_slots
q "BEGIN;
   INSERT INTO orders(id,status,note) SELECT g,'bulk',repeat('x',40) FROM generate_series(1,8000) g;
   COMMIT;" >/dev/null
echo '--- pgoutput message-type histogram (49=Insert 53=S 45=E 63=c 52=R 59=Y) ---'; hist
echo '--- Stream Start frames: first_segment flag (1 once, 0 after) ---'
q "SELECT encode(data,'hex') FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','on') WHERE left(encode(data,'hex'),2)='53'" \
  | decode | awk '{print $3,$4}' | sort | uniq -c

hdr "5. SUBTRANSACTION ROLLBACK inside a committed txn (Stream Abort sub!=top)"
reset_slots
q "BEGIN;
     INSERT INTO orders(id,status,note) SELECT 20000+g,'kept-A',repeat('a',40) FROM generate_series(1,3000) g;
     SAVEPOINT sp1;
     INSERT INTO orders(id,status,note) SELECT 30000+g,'ROLLED-BACK',repeat('b',40) FROM generate_series(1,3000) g;
     ROLLBACK TO sp1;
     INSERT INTO orders(id,status,note) SELECT 40000+g,'kept-B',repeat('c',40) FROM generate_series(1,3000) g;
   COMMIT;" >/dev/null
echo '--- Stream Abort + Stream Commit frames (decoded) ---'
q "SELECT encode(data,'hex') FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','on') WHERE left(encode(data,'hex'),2) IN ('41','63')" | decode
echo '--- per-message xid on streamed INSERTs (= SUBxact xid; the aborted sub is discarded) ---'
q "SELECT encode(data,'hex') FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','on') WHERE left(encode(data,'hex'),2)='49'" \
  | python3 -c "import sys;from collections import Counter;c=Counter(int.from_bytes(bytes.fromhex(l.strip())[1:5],'big') for l in sys.stdin if l.strip());[print(f'  INSERT xid={x}: {n}') for x,n in sorted(c.items())]"

hdr "6. TWO CONCURRENT LARGE TXNS -> interleaved blocks; commit order != start order"
reset_slots
q "BEGIN; INSERT INTO orders(id,status,note) SELECT 100000+g,'txnA',repeat('A',30) FROM generate_series(1,6000) g; COMMIT;" >/dev/null &
q "BEGIN; INSERT INTO orders(id,status,note) SELECT 200000+g,'txnB',repeat('B',30) FROM generate_series(1,6000) g; COMMIT;" >/dev/null &
wait
echo '--- Stream Start sequence (xids alternate on the wire) ---'
q "SELECT encode(data,'hex') FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','on') WHERE left(encode(data,'hex'),2)='53'" \
  | decode | awk '{print $3,$4}' | head -8
echo '--- Stream Commit order (may differ from start order -> order by commit_lsn) ---'
q "SELECT encode(data,'hex') FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','on') WHERE left(encode(data,'hex'),2)='63'" | decode

hdr "7. PROTOCOL AXIS: v1 vs v2, streaming rejection, streaming=off buffering"
reset_slots
q "INSERT INTO customers(region,id,name) VALUES ('eu',7,'Zeta');" >/dev/null
echo '--- v1 + streaming=on -> ERROR (streaming needs v2+) ---'
psql -c "SELECT 1 FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,'proto_version','1','publication_names','pub','streaming','on');" 2>&1 | grep -i error || true
echo '--- v2 + streaming=off on a large txn -> buffered whole, NO 53/45/63 frames ---'
q "BEGIN; INSERT INTO orders(id,status,note) SELECT 300000+g,'nostream',repeat('z',30) FROM generate_series(1,4000) g; COMMIT;" >/dev/null
q "SELECT left(encode(data,'hex'),2) b, count(*) FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','off') GROUP BY 1 ORDER BY 2 DESC;"

hdr "8. LIVE WALSENDER: whole-txn abort emits a REAL Stream Abort (sub==top)"
echo '(The SQL-function path short-circuits an already-aborted txn — empty block, no Abort frame.'
echo ' A live walsender streams the rows BEFORE the abort is known, so it DOES emit Stream Abort.'
echo ' This is the path walrus-pg-sink uses, so it must discard the streamed rows on that frame.)'
q "SELECT pg_drop_replication_slot('slot_live') FROM pg_replication_slots WHERE slot_name='slot_live';" >/dev/null 2>&1 || true
q "SELECT slot_name FROM pg_create_logical_replication_slot('slot_live','pgoutput');" >/dev/null
docker exec "$CID" rm -f /tmp/live.bin
# pg_recvlogical opens a real START_REPLICATION connection and streams to a file.
docker exec -d "$CID" bash -c "pg_recvlogical -U postgres -d walrus --slot=slot_live --start -o proto_version=2 -o publication_names=pub -o streaming=on -f /tmp/live.bin --fsync-interval=100"
sleep 2
psql >/dev/null 2>&1 <<'SQL'
BEGIN;
  INSERT INTO orders(id,status,note) SELECT 500000+g,'live-abort',repeat('q',40) FROM generate_series(1,6000) g;
  SELECT pg_sleep(2);      -- let the walsender stream the in-progress rows before we roll back
ROLLBACK;
-- a committed record AFTER the abort pushes the walsender past + flushes the Abort frame
INSERT INTO customers(region,id,name) VALUES ('flush', 999, 'x');
SQL
sleep 3
docker exec "$CID" bash -c "pkill -INT pg_recvlogical" 2>/dev/null || true
sleep 1
echo '--- frame summary from the live byte stream (note: rows WERE streamed, then aborted) ---'
docker exec -i "$CID" cat /tmp/live.bin | decode_stream 2>/dev/null \
  | grep -E 'STREAM (START|ABORT)|^INSERT' | sed -E 's/xid=[0-9]+/xid=N/; s/first_segment=[01]/first=./; s/rel=[0-9]+.*/.../' \
  | sort | uniq -c
echo '--- the Stream Abort frame (the aborted txn walrus must discard) ---'
docker exec -i "$CID" cat /tmp/live.bin | decode_stream 2>/dev/null | grep 'STREAM ABORT'
q "SELECT pg_drop_replication_slot('slot_live');" >/dev/null 2>&1 || true

printf '\n\033[1mDone.\033[0m Slots left intact (peeked, not consumed) except resets. Teardown: docker compose down -v\n'
