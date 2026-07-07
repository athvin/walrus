-- 0002_ddl_triggers.sql — the sink's DDL tap on the source (§3, PR 2.33). Idempotent, re-runnable.
--
-- Postgres logical decoding NEVER emits DDL. So the source carries an event-trigger tap: an INSERT
-- into the PUBLISHED walrus.ddl_audit table rides the *same* replication slot as DML, in commit order
-- (the `ddl_command_end` trigger fires after execution but pre-commit, so the audit INSERT enters the
-- WAL in the same transaction as the DDL — that is the inline commit-order guarantee, no separate poll).
-- The sink consumes the decoded INSERT (schema-DIFF from the structured c_columns jsonb — NOT a replay
-- of c_ddl_text), writes a ddl_manifest row, bumps the table's structural schema_version, and cuts a
-- fresh Parquet file. Superuser is needed to CREATE EVENT TRIGGER; the functions are SECURITY DEFINER
-- so a non-owner's DDL still writes the protected audit table.

-- Extend the 0001 stub with the columns the sink reads (idempotent).
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_schema  text;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_table   text;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_columns jsonb;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_dropped jsonb;

-- A structured snapshot of a relation's live columns — the schema-diff INPUT (read the ALREADY-changed
-- catalog, since ddl_command_end fires post-execution).
CREATE OR REPLACE FUNCTION walrus.snapshot_columns(relid oid) RETURNS jsonb
LANGUAGE sql SECURITY DEFINER AS $$
  SELECT COALESCE(
    jsonb_agg(jsonb_build_object(
      'name', a.attname,
      'type_oid', a.atttypid::int8,
      'type_modifier', a.atttypmod,
      'not_null', a.attnotnull,
      'attnum', a.attnum
    ) ORDER BY a.attnum),
    '[]'::jsonb)
  FROM pg_attribute a
  WHERE a.attrelid = relid AND a.attnum > 0 AND NOT a.attisdropped;
$$;

-- `ddl_command_end`: capture ALTER/CREATE/COMMENT on plain/partitioned USER tables. c_lsn orders the
-- event against data; c_columns is the post-change shape.
CREATE OR REPLACE FUNCTION walrus.intercept_ddl() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  r        record;
  v_schema text;
  v_table  text;
BEGIN
  FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    SELECT n.nspname, c.relname INTO v_schema, v_table
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = r.objid AND c.relkind IN ('r', 'p');
    CONTINUE WHEN v_table IS NULL;      -- not a plain/partitioned table
    CONTINUE WHEN v_schema = 'walrus';   -- our own internal tables are never audited
    INSERT INTO walrus.ddl_audit (c_lsn, c_event, c_tag, c_schema, c_table, c_columns)
    VALUES (pg_current_wal_lsn(), 'ddl_command_end', r.command_tag, v_schema, v_table,
            walrus.snapshot_columns(r.objid));
  END LOOP;
END;
$$;

-- `sql_drop`: capture dropped tables/columns (the loader applies destructive changes — PR 3.9). Only
-- the dropped identity is needed; the ALTER itself is visible via ddl_command_end.
CREATE OR REPLACE FUNCTION walrus.intercept_drop() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
    CONTINUE WHEN r.schema_name IS NULL OR r.schema_name = 'walrus';
    IF r.object_type = 'table' THEN
      INSERT INTO walrus.ddl_audit (c_lsn, c_event, c_tag, c_schema, c_table, c_dropped)
      VALUES (pg_current_wal_lsn(), 'sql_drop', 'DROP TABLE', r.schema_name, r.object_name,
              jsonb_build_object('object_type', r.object_type, 'identity', r.object_identity));
    ELSIF r.object_type = 'table column' THEN
      INSERT INTO walrus.ddl_audit (c_lsn, c_event, c_tag, c_schema, c_table, c_dropped)
      VALUES (pg_current_wal_lsn(), 'sql_drop', 'DROP COLUMN', r.schema_name, split_part(r.object_identity, '.', 2),
              jsonb_build_object('object_type', r.object_type, 'identity', r.object_identity));
    END IF;
  END LOOP;
END;
$$;

DROP EVENT TRIGGER IF EXISTS walrus_intercept_ddl;
CREATE EVENT TRIGGER walrus_intercept_ddl ON ddl_command_end
  EXECUTE FUNCTION walrus.intercept_ddl();

DROP EVENT TRIGGER IF EXISTS walrus_intercept_drop;
CREATE EVENT TRIGGER walrus_intercept_drop ON sql_drop
  EXECUTE FUNCTION walrus.intercept_drop();

-- Table-list publications must add the audit table explicitly; a FOR ALL TABLES dev publication already
-- covers it. Guard so re-running is a no-op regardless of the publication shape.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'walrus_pub' AND NOT puballtables)
     AND NOT EXISTS (
       SELECT 1 FROM pg_publication_tables
       WHERE pubname = 'walrus_pub' AND schemaname = 'walrus' AND tablename = 'ddl_audit')
  THEN
    ALTER PUBLICATION walrus_pub ADD TABLE walrus.ddl_audit;
  END IF;
END $$;
