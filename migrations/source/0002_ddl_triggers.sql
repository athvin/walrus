-- 0002_ddl_triggers.sql — the sink's DDL tap on the source (§3). Idempotent, re-runnable.
--
-- Postgres logical decoding NEVER emits DDL. So the source carries an event-trigger tap: an INSERT
-- into the PUBLISHED walrus.ddl_audit table rides the *same* replication slot and transaction as DML.
-- The sink commit-gates that INSERT: its ddl_manifest row uses the transaction's actual Commit /
-- StreamCommit LSN, and an aborted streamed transaction leaves no DDL state behind.
--
-- c_columns is the authoritative post-change catalog snapshot used for schema diffs. c_ddl_text is
-- supplemental audit context only: current_query() can contain a multi-statement batch, dynamic SQL,
-- search_path-dependent names, or syntax a downstream parser does not implement, so correctness must
-- never depend on replaying or parsing it. Superuser is needed to CREATE EVENT TRIGGER; the function is
-- SECURITY DEFINER so a non-owner's DDL still writes the protected audit table.

-- Extend the 0001 stub with the columns the sink reads (idempotent).
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_schema  text;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_table   text;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_columns jsonb;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_dropped jsonb;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_rel_oid oid;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_replica_identity text;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_ddl_text text;

-- A structured snapshot of a relation's live columns — the schema-diff INPUT (read the ALREADY-changed
-- catalog, since ddl_command_end fires post-execution).
CREATE OR REPLACE FUNCTION walrus.snapshot_columns(relid oid) RETURNS jsonb
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
  SELECT COALESCE(
    jsonb_agg(jsonb_build_object(
      'name', a.attname,
      'type_oid', a.atttypid::int8,
      'type_modifier', a.atttypmod,
      'is_key', c.relreplident = 'f' OR EXISTS (
        SELECT 1
        FROM pg_index i
        WHERE i.indrelid = a.attrelid
          AND (i.indisreplident OR (c.relreplident = 'd' AND i.indisprimary))
          AND a.attnum = ANY(i.indkey)
      ),
      'not_null', a.attnotnull,
      'attnum', a.attnum
    ) ORDER BY a.attnum),
    '[]'::jsonb)
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  WHERE a.attrelid = relid AND a.attnum > 0 AND NOT a.attisdropped;
$$;

-- One implementation serves both event kinds. PostgreSQL still requires TWO event-trigger bindings:
-- CREATE EVENT TRIGGER accepts exactly one event, and the event-specific SRFs are only valid while
-- handling their corresponding event. ddl_command_end supplies the surviving post-change relation;
-- sql_drop is required for a table that no longer exists by ddl_command_end.
CREATE OR REPLACE FUNCTION walrus.intercept_ddl() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
  r        record;
  v_schema text;
  v_table  text;
BEGIN
  IF TG_EVENT = 'ddl_command_end' THEN
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
      SELECT n.nspname, c.relname INTO v_schema, v_table
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.oid = r.objid AND c.relkind IN ('r', 'p');
      CONTINUE WHEN v_table IS NULL;       -- not a surviving plain/partitioned table
      CONTINUE WHEN v_schema = 'walrus';  -- internal control tables are never audited
      INSERT INTO walrus.ddl_audit
        (c_lsn, c_event, c_tag, c_schema, c_table, c_rel_oid, c_replica_identity,
         c_columns, c_ddl_text)
      SELECT pg_current_wal_lsn(), 'ddl_command_end', r.command_tag, v_schema, v_table,
             c.oid, c.relreplident::text, walrus.snapshot_columns(c.oid), current_query()
      FROM pg_class c
      WHERE c.oid = r.objid;
    END LOOP;
  ELSIF TG_EVENT = 'sql_drop' THEN
    FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
      CONTINUE WHEN r.schema_name IS NULL OR r.schema_name = 'walrus';
      -- DROP COLUMN is represented authoritatively by ddl_command_end's surviving post-change table
      -- snapshot. Recording it here too would create two schema-version bumps for one ALTER TABLE.
      IF r.object_type = 'table' THEN
        INSERT INTO walrus.ddl_audit
          (c_lsn, c_event, c_tag, c_schema, c_table, c_rel_oid, c_columns, c_dropped,
           c_ddl_text)
        VALUES
          (pg_current_wal_lsn(), 'sql_drop', 'DROP TABLE', r.schema_name, r.object_name,
           r.objid, '[]'::jsonb,
           jsonb_build_object('object_type', r.object_type, 'identity', r.object_identity),
           current_query());
      END IF;
    END LOOP;
  END IF;
END;
$$;

DROP EVENT TRIGGER IF EXISTS walrus_intercept_ddl;
CREATE EVENT TRIGGER walrus_intercept_ddl ON ddl_command_end
  EXECUTE FUNCTION walrus.intercept_ddl();

DROP EVENT TRIGGER IF EXISTS walrus_intercept_drop;
CREATE EVENT TRIGGER walrus_intercept_drop ON sql_drop
  EXECUTE FUNCTION walrus.intercept_ddl();

DROP FUNCTION IF EXISTS walrus.intercept_drop();

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
