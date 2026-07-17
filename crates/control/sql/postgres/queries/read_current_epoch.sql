SELECT epoch, slot_name, created_lsn AS "created_lsn: Lsn", status
FROM walrus.replication_state
ORDER BY epoch DESC
LIMIT 1
