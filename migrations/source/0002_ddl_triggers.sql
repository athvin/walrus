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

-- Serialize an install/reapply with every online sink before touching the trigger that enforces
-- the same protocol. Keeping the whole file in this transaction makes the xact lock span the later
-- DROP/CREATE EVENT TRIGGER statements; on any error PostgreSQL rolls all replacements back.
BEGIN;
SELECT pg_catalog.pg_advisory_xact_lock(8602276002106929250);

-- Extend the 0001 stub with the columns the sink reads (idempotent).
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_schema  text;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_table   text;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_columns jsonb;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_dropped jsonb;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_rel_oid oid;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_replica_identity text;
ALTER TABLE walrus.ddl_audit ADD COLUMN IF NOT EXISTS c_ddl_text text;

-- Remove every binding this migration replaces before installing the command-start guard. PostgreSQL
-- does not expose ALTER/DROP EVENT TRIGGER as event-trigger command tags; leaving an older guard live
-- while rebuilding the other bindings would also make an idempotent reapply depend on itself.
DROP EVENT TRIGGER IF EXISTS walrus_guard_publication_ddl;
DROP EVENT TRIGGER IF EXISTS walrus_intercept_ddl;
DROP EVENT TRIGGER IF EXISTS walrus_intercept_drop;

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
          AND EXISTS (
            SELECT 1
            FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
            WHERE k.attnum = a.attnum AND k.ord <= i.indnkeyatts
          )
      ),
      'not_null', a.attnotnull,
      'attnum', a.attnum
    ) ORDER BY a.attnum),
    '[]'::jsonb)
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  -- pgoutput omits generated columns from Relation and tuple messages. The audit snapshot feeds the
  -- same registry shape, so it must use that exact published column set too.
  WHERE a.attrelid = relid AND a.attnum > 0 AND NOT a.attisdropped
    AND a.attgenerated = '';
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
  r               record;
  v_schema        text;
  v_schema_before text;
  v_table         text;
BEGIN
  IF TG_EVENT = 'ddl_command_end' THEN
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
      -- ALTER SCHEMA emits no per-table command for the relations whose qualified identities just
      -- changed. The command-start guard snapshots published namespace OID -> name in a transaction-
      -- local setting. Comparing that stable OID here distinguishes RENAME from metadata-only ALTER
      -- SCHEMA forms without parsing current_query(). Raising at command end rolls the rename back.
      IF r.command_tag = 'ALTER SCHEMA' THEN
        SELECT n.nspname INTO v_schema
        FROM pg_namespace n
        WHERE n.oid = r.objid;
        v_schema_before := (
          NULLIF(current_setting('walrus.published_schema_identity_snapshot', true), '')::jsonb
          ->> r.objid::text
        );
        IF v_schema_before IS NOT NULL
           AND v_schema_before IS DISTINCT FROM v_schema
           AND NOT pg_try_advisory_xact_lock(8602276002106929250)
        THEN
          RAISE EXCEPTION
            'published schema identity change rejected while Walrus replication is online'
            USING ERRCODE = '55P03',
                  HINT = 'stop the Walrus sink before renaming or altering a published schema';
        END IF;
        CONTINUE;
      END IF;
      SELECT n.nspname, c.relname INTO v_schema, v_table
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.oid = r.objid AND c.relkind IN ('r', 'p');
      CONTINUE WHEN v_table IS NULL;       -- not a surviving plain/partitioned table
      CONTINUE WHEN v_schema = 'walrus';  -- internal control tables are never audited
      -- A FOR ALL TABLES or schema publication can acquire a brand-new target without executing
      -- ALTER PUBLICATION. At command end the catalog/view already exposes that effective
      -- membership inside this transaction. Reject only those automatically-published creations
      -- while a sink owns the shared coverage guard; raising rolls the whole CREATE back. Temp and
      -- otherwise-unpublished tables remain unrestricted.
      IF r.command_tag IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
         AND EXISTS (
           SELECT 1
           FROM pg_publication_tables pt
           WHERE pt.schemaname = v_schema AND pt.tablename = v_table
         )
         AND NOT pg_try_advisory_xact_lock(8602276002106929250)
      THEN
        RAISE EXCEPTION
          'automatically published table creation rejected while Walrus replication is online'
          USING ERRCODE = '55P03',
                HINT = 'stop the Walrus sink or use an explicit table-list publication';
      END IF;
      -- Preflight accepts only topology-independent targets. Preserve that condition online: an
      -- ALTER TABLE that attaches a published table as a partition/inheritance child (or makes a
      -- published relation a parent) would otherwise change its effective row/WAL coverage without
      -- ALTER PUBLICATION. DETACH/NO INHERIT can only start from a state preflight already rejects.
      IF r.command_tag = 'ALTER TABLE'
         AND EXISTS (
           SELECT 1
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE (
               c.oid = r.objid
               OR c.oid IN (
                 SELECT i.inhrelid FROM pg_inherits i WHERE i.inhparent = r.objid
               )
               OR c.oid IN (
                 SELECT i.inhparent FROM pg_inherits i WHERE i.inhrelid = r.objid
               )
             )
             AND (
               c.relkind = 'p'
               OR c.relispartition
               OR EXISTS (
                 SELECT 1 FROM pg_inherits i
                 WHERE i.inhrelid = c.oid OR i.inhparent = c.oid
               )
             )
             AND EXISTS (
               SELECT 1
               FROM pg_publication_tables pt
               WHERE pt.schemaname = n.nspname AND pt.tablename = c.relname
             )
         )
         AND NOT pg_try_advisory_xact_lock(8602276002106929250)
      THEN
        RAISE EXCEPTION
          'published table topology change rejected while Walrus replication is online'
          USING ERRCODE = '55P03',
                HINT = 'stop the Walrus sink before changing partition/inheritance topology';
      END IF;
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

-- Full-table reconciliation assumes publication coverage is continuous from its start fence F
-- through its end fence H. Catalog checks at F and H alone cannot detect DELETE being disabled,
-- a row changing, and DELETE being re-enabled entirely between those checks. Every exporter
-- therefore runs under the sink pipeline's matching shared SESSION advisory locks. This command-
-- start trigger tries the exclusive TRANSACTION lock before publication DDL can mutate the
-- catalogs. The command is rejected immediately while any sink is online. Automatically-published
-- CREATE commands are checked selectively by intercept_ddl above, once the exact new relation and
-- its effective membership are visible. Commit/rollback releases a successful offline acquisition.
-- The fixed bigint is ASCII `walruspb` and is duplicated as PUBLICATION_DDL_GUARD_KEY in
-- crates/pg-sink/src/source_catalog.rs.
CREATE OR REPLACE FUNCTION walrus.guard_publication_ddl() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
  IF TG_TAG IN (
    'CREATE PUBLICATION', 'ALTER PUBLICATION', 'DROP PUBLICATION'
  ) THEN
    IF NOT pg_try_advisory_xact_lock(8602276002106929250) THEN
      RAISE EXCEPTION
        'publication DDL rejected while Walrus replication holds its coverage guard'
        USING ERRCODE = '55P03',
              HINT = 'stop the Walrus sink before changing publication configuration';
    END IF;
  ELSIF TG_TAG = 'ALTER SCHEMA' THEN
    -- ddl_command_start cannot expose the target command object, so snapshot every namespace that
    -- currently contains a published table. ddl_command_end resolves the one stable namespace OID
    -- from its command payload and compares names. Transaction-local state vanishes on either fate.
    PERFORM set_config(
      'walrus.published_schema_identity_snapshot',
      COALESCE((
        SELECT jsonb_object_agg(published.oid::text, published.nspname)::text
        FROM (
          SELECT DISTINCT n.oid, n.nspname
          FROM pg_namespace n
          JOIN pg_publication_tables pt ON pt.schemaname = n.nspname
        ) published
      ), '{}'),
      true
    );
  END IF;
END;
$$;

CREATE EVENT TRIGGER walrus_intercept_ddl ON ddl_command_end
  EXECUTE FUNCTION walrus.intercept_ddl();

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

-- Install the command-start binding last, after this migration's own possible ALTER PUBLICATION.
-- An idempotent reapply always removes this binding before rebuilding the other event triggers.
CREATE EVENT TRIGGER walrus_guard_publication_ddl ON ddl_command_start
  WHEN TAG IN ('CREATE PUBLICATION', 'ALTER PUBLICATION', 'DROP PUBLICATION', 'ALTER SCHEMA')
  EXECUTE FUNCTION walrus.guard_publication_ddl();

COMMIT;
