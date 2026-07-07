-- 0001_control_schema.sql — the walrus coordination contract as SQL.
--
-- Mirrors architecture.md "Coordination contract (control-plane tables)". The five tables are the
-- hand-off between the sink and the loader. Correctness lives in the DDL, not in comments:
--   * every LSN column is `pg_lsn` (sorts as a WAL position) and every progress/watermark value is
--     a COMMIT LSN, never a max-row LSN (see the note in architecture.md — ordering by max-row LSN
--     would silently drop a late-committing large transaction);
--   * `file_manifest` is a WORK QUEUE, not a history: an applied row is DELETED (not flipped to a
--     terminal state), so the partial claim index only needs to cover `status = 'ready'` rows;
--   * `ddl_manifest` / `schema_registry` are NEVER pruned — they're the low-volume schema history
--     needed to reconstruct any table at any `schema_version`.

CREATE SCHEMA IF NOT EXISTS walrus;

-- One row per slot lifetime; a new slot = a new epoch (see §1.8).
CREATE TABLE walrus.replication_state (
  epoch        bigint PRIMARY KEY,        -- monotonic generation id
  slot_name    text NOT NULL,
  created_lsn  pg_lsn NOT NULL,           -- consistent snapshot LSN at slot creation
  status       text NOT NULL,            -- bootstrapping | streaming | total_restart
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE walrus.file_manifest (
  id             bigserial PRIMARY KEY,   -- also the tiebreaker for equal-lsn_end files (snapshot)
  epoch          bigint NOT NULL,         -- FK -> replication_state; namespaces ALL state
  source_schema  text NOT NULL,
  source_table   text NOT NULL,
  s3_uri         text NOT NULL,
  kind           text NOT NULL,           -- 'snapshot' | 'stream'
  row_count      bigint NOT NULL,
  lsn_start      pg_lsn NOT NULL,         -- commit LSN of the file's first txn
  lsn_end        pg_lsn NOT NULL,         -- COMMIT LSN of the file's last txn (NOT max row lsn)
  schema_version bigint NOT NULL,         -- FK -> schema_registry
  status         text NOT NULL DEFAULT 'ready',  -- ready | failed  (applied rows are DELETED, not kept)
  created_at     timestamptz NOT NULL DEFAULT now()
);
-- The loader's claim query orders by (lsn_end, id) over ready rows only; a PARTIAL index keeps it
-- tight because applied rows leave the queue.
CREATE INDEX file_manifest_claim_idx
  ON walrus.file_manifest (epoch, source_schema, source_table, lsn_end, id)
  WHERE status = 'ready';

CREATE TABLE walrus.loader_checkpoint (
  epoch            bigint NOT NULL,
  source_schema    text NOT NULL,
  source_table     text NOT NULL,
  raw_appended_lsn pg_lsn NOT NULL,     -- Phase A: CDC log durable up to this COMMIT LSN
  transformed_lsn  pg_lsn NOT NULL,     -- Phase B: mirror derived up to this COMMIT LSN
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (epoch, source_schema, source_table),
  CHECK (transformed_lsn <= raw_appended_lsn)  -- the mirror can never be ahead of the raw log
);

-- Versioned per-column type-mapping descriptors (common::TypeDescriptor, PR 1.2). One row per
-- structural schema version of a table; NEVER pruned. Column detail is refined in PR 1.6.
CREATE TABLE walrus.schema_registry (
  epoch          bigint NOT NULL,
  source_schema  text NOT NULL,
  source_table   text NOT NULL,
  schema_version bigint NOT NULL,
  descriptors    jsonb NOT NULL,        -- array of per-column TypeDescriptor
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (epoch, source_schema, source_table, schema_version)
);

-- Schema-change events captured from the source's ddl_audit stream, stamped with the DDL's commit
-- LSN so the loader applies each change at the right point in the data stream. NEVER pruned.
-- Column detail is refined in PR 1.6.
CREATE TABLE walrus.ddl_manifest (
  id             bigserial PRIMARY KEY,
  epoch          bigint NOT NULL,
  source_schema  text NOT NULL,
  source_table   text NOT NULL,
  schema_version bigint NOT NULL,        -- the version this DDL produces
  lsn            pg_lsn NOT NULL,        -- commit LSN of the DDL event (orders it against data)
  change         jsonb NOT NULL,         -- the structural change
  created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ddl_manifest_apply_idx
  ON walrus.ddl_manifest (epoch, source_schema, source_table, lsn, id);
