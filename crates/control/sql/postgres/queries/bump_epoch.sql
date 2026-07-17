INSERT INTO walrus.replication_state (epoch, slot_name, created_lsn, status)
SELECT COALESCE(MAX(epoch), 0) + 1, $1, $2, $3
FROM walrus.replication_state
RETURNING epoch
