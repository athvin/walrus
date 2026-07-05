-- 02-commands.sql — a copy-paste cheatsheet for poking at the slots by hand.
-- For the full, labeled reproduction use ./03-capture.sh instead.
--
--   docker exec -it walrus-proto-pg psql -U postgres -d walrus
--   \i /dev/stdin      (or just paste the blocks you want)

-- ===========================================================================
-- CAPTURE QUERIES  (peek = non-consuming; get = consuming/advances the slot)
-- ===========================================================================

-- test_decoding, human-readable. 'stream-changes','1' shows the streamed-block markers.
SELECT lsn, xid, data FROM pg_logical_slot_peek_changes(
  'slot_test', NULL, NULL, 'include-xids','1', 'include-timestamp','on', 'stream-changes','1');

-- pgoutput, binary. It is NOT text — hex-encode it, then pipe through decode_pgoutput.py.
-- 'streaming','on' requires 'proto_version' >= '2'. 'messages','on' surfaces logical messages.
SELECT lsn, xid, encode(data,'hex') AS hex FROM pg_logical_slot_peek_binary_changes(
  'slot_pg', NULL, NULL,
  'proto_version','2', 'publication_names','pub', 'streaming','on', 'messages','on');

-- Consume (advance) when you want a clean slate for the next experiment:
--   SELECT count(*) FROM pg_logical_slot_get_changes('slot_test',NULL,NULL,'stream-changes','1');
--   SELECT count(*) FROM pg_logical_slot_get_binary_changes('slot_pg',NULL,NULL,
--          'proto_version','2','publication_names','pub','streaming','on');

-- Slot state (watch restart_lsn vs confirmed_flush_lsn):
SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots ORDER BY slot_name;

-- ===========================================================================
-- THE COMMAND MATRIX  (run a block, then run a capture query above)
-- ===========================================================================

-- (1) Basic DML — DEFAULT identity: UPDATE/DELETE carry only the key.
INSERT INTO orders(id,status,amount,feeling,note) VALUES (1,'new',19.99,'happy','first');
UPDATE orders SET status='shipped', amount=29.99 WHERE id=1;
UPDATE orders SET id=100 WHERE id=1;      -- PK change -> 'K' old-key submessage
DELETE FROM orders WHERE id=100;

-- (2) FULL identity: UPDATE/DELETE carry the WHOLE old row ('O').
INSERT INTO items(id,label,qty) VALUES (1,'widget',5);
UPDATE items SET qty=9 WHERE id=1;
DELETE FROM items WHERE id=1;

-- (3) Composite PK, TRUNCATE variants, logical MESSAGE (walrus heartbeat).
INSERT INTO customers(region,id,name) VALUES ('us',1,'Acme');
TRUNCATE orders;                          -- opts=none
TRUNCATE items RESTART IDENTITY CASCADE;  -- opts=CASCADE|RESTART IDENTITY
SELECT pg_logical_emit_message(true ,'walrus','heartbeat-txn');     -- transactional
SELECT pg_logical_emit_message(false,'walrus','heartbeat-nontxn');  -- non-transactional

-- (4) Unchanged-TOAST ('u'): the value is NOT re-sent.
ALTER TABLE orders ALTER COLUMN note SET STORAGE EXTERNAL;  -- force out-of-line TOAST
INSERT INTO orders(id,status,note) VALUES (888,'new',repeat('T',5000));
UPDATE orders SET status='changed' WHERE id=888;           -- note unchanged -> 'u'

-- (5) Large committed txn -> streamed in blocks (needs logical_decoding_work_mem small).
BEGIN;
  INSERT INTO orders(id,status,note) SELECT g,'bulk',repeat('x',40) FROM generate_series(1,8000) g;
COMMIT;

-- (6) Subtransaction rollback inside a committed txn -> Stream Abort with sub_xid != top_xid.
BEGIN;
  INSERT INTO orders(id,status,note) SELECT 20000+g,'kept-A',repeat('a',40) FROM generate_series(1,3000) g;
  SAVEPOINT sp1;
  INSERT INTO orders(id,status,note) SELECT 30000+g,'ROLLED-BACK',repeat('b',40) FROM generate_series(1,3000) g;
  ROLLBACK TO sp1;
  INSERT INTO orders(id,status,note) SELECT 40000+g,'kept-B',repeat('c',40) FROM generate_series(1,3000) g;
COMMIT;

-- (7) Aborted large txn. Via THIS SQL path it short-circuits (concurrent-abort);
--     over a live pg_recvlogical walsender it streams then emits a real Stream Abort.
BEGIN;
  INSERT INTO orders(id,status,note) SELECT 10000+g,'doomed',repeat('y',40) FROM generate_series(1,8000) g;
ROLLBACK;
