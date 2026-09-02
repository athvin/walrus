-- Durable loader publication ownership and recovery state.
--
-- `export_complete` means the exporter made the bounded [F,H] input durable. The loader must
-- first claim that immutable attempt as `publishing`, fenced by its table-ownership lease, before
-- it may build or publish the hidden DuckLake generation. `complete` is written only after the
-- DuckLake swap has committed and the canonical loader checkpoint is advanced to H.

ALTER TABLE walrus.table_reload
  DROP CONSTRAINT table_reload_status_check;

ALTER TABLE walrus.table_reload
  ADD CONSTRAINT table_reload_status_check
    CHECK (status IN (
      'requested', 'exporting', 'export_complete', 'publishing', 'complete', 'failed'
    )),
  ADD COLUMN publication_nonce uuid,
  ADD COLUMN publisher_owner_pod text,
  ADD COLUMN publisher_fencing_token bigint,
  ADD COLUMN publishing_at timestamptz,
  ADD COLUMN publication_error text,
  ADD COLUMN publication_error_at timestamptz,
  ADD CONSTRAINT table_reload_publishing_identity_check
    CHECK (
      status <> 'publishing'
      OR (
        publication_nonce IS NOT NULL
        AND publisher_owner_pod IS NOT NULL
        AND publisher_fencing_token IS NOT NULL
        AND publishing_at IS NOT NULL
      )
    );

-- Exporter ownership is a fencing token, not just a friendly lease label. Snapshot identifiers
-- are persisted for audit/recovery decisions, but can only be imported while the transaction that
-- exported them is alive; a new generation must supersede a lost snapshot rather than reuse it.
ALTER TABLE walrus.table_reload
  ADD COLUMN exporter_generation bigint NOT NULL DEFAULT 0,
  ADD COLUMN export_snapshot text,
  ADD COLUMN export_snapshot_xmin bigint,
  ADD COLUMN export_snapshot_xmax bigint,
  ADD COLUMN export_range_count bigint,
  ADD COLUMN export_sealed_at timestamptz,
  ADD COLUMN export_file_count bigint,
  ADD COLUMN export_row_count bigint,
  ADD CONSTRAINT table_reload_exporter_generation_check CHECK (exporter_generation >= 0),
  ADD CONSTRAINT table_reload_snapshot_shape_check CHECK (
    (export_snapshot IS NULL
      AND export_snapshot_xmin IS NULL
      AND export_snapshot_xmax IS NULL
      AND export_range_count IS NULL)
    OR
    (export_snapshot IS NOT NULL
      AND export_snapshot_xmin IS NOT NULL
      AND export_snapshot_xmax IS NOT NULL
      AND export_snapshot_xmin <= export_snapshot_xmax
      AND export_range_count IS NOT NULL
      AND export_range_count > 0)
  ),
  ADD CONSTRAINT table_reload_export_seal_shape_check CHECK (
    (export_sealed_at IS NULL AND export_file_count IS NULL AND export_row_count IS NULL)
    OR
    (export_sealed_at IS NOT NULL
      AND export_file_count IS NOT NULL AND export_file_count >= 0
      AND export_row_count IS NOT NULL AND export_row_count >= 0)
  ),
  ADD CONSTRAINT table_reload_completed_export_is_sealed_check CHECK (
    status NOT IN ('export_complete', 'publishing', 'complete')
    OR exporter_generation = 0
    OR
    (export_snapshot IS NOT NULL
      AND export_sealed_at IS NOT NULL
      AND start_lsn IS NOT NULL
      AND final_lsn IS NOT NULL
      AND start_lsn <= final_lsn)
  ),
  ADD CONSTRAINT table_reload_v2_complete_identity_check CHECK (
    status <> 'complete'
    OR exporter_generation = 0
    OR (
      publication_nonce IS NOT NULL
      AND publisher_owner_pod IS NOT NULL
      AND publisher_fencing_token IS NOT NULL
      AND publishing_at IS NOT NULL
    )
  );

-- A pre-v2 exporter embeds a claim statement that leaves exporter_generation at zero. Reject its
-- transition, and reject any subsequent generation-zero mutation that tries to remain exporting.
-- A v2 adoption increments the generation before doing further work, so it is admitted.
CREATE FUNCTION walrus.guard_reload_exporter_protocol_v2()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.status = 'exporting' AND NEW.exporter_generation <= 0 THEN
    RAISE EXCEPTION
      'reload export requires a protocol-v2 exporter fencing generation'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_exporter_protocol_v2';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER table_reload_exporter_protocol_v2
BEFORE INSERT OR UPDATE OF status, exporter_generation, chunk_no, cursor_pk,
                           start_lsn, schema_version, export_snapshot, export_sealed_at
ON walrus.table_reload
FOR EACH ROW EXECUTE FUNCTION walrus.guard_reload_exporter_protocol_v2();

-- Crossing into `exporting`, or explicitly adopting an already-exporting lease, must mint a
-- strictly newer generation. `UPDATE OF lease_holder` fires even when a pre-v2 adopter writes the
-- same holder value, so a rolling old process cannot reuse a positive generation left behind by a
-- v2 release or adoption. Ordinary v2 renew/progress statements do not assign these acquisition
-- columns; v2 claim/adoption assigns them together with `exporter_generation + 1`.
CREATE FUNCTION walrus.guard_reload_exporter_acquisition_v2()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.status = 'exporting'
     AND NEW.exporter_generation <= OLD.exporter_generation THEN
    RAISE EXCEPTION
      'claiming or adopting a reload export must mint a newer protocol-v2 fencing generation'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_exporter_acquisition_v2';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER table_reload_exporter_acquisition_v2
BEFORE UPDATE OF status, exporter_generation, lease_holder
ON walrus.table_reload
FOR EACH ROW EXECUTE FUNCTION walrus.guard_reload_exporter_acquisition_v2();

-- SQL is embedded in binaries, so a still-running pre-v2 loader could otherwise execute its old
-- unguarded `export_complete -> complete` statement after this migration. Reject that transition
-- in the database; modern completion must first own `publishing` and its fenced nonce.
CREATE FUNCTION walrus.guard_reload_v2_completion()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.status = 'complete'
     AND OLD.status = 'export_complete'
     AND OLD.exporter_generation > 0 THEN
    RAISE EXCEPTION
      'protocol-v2 reload must pass through fenced publishing before complete'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_v2_completion_guard';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER table_reload_v2_completion_guard
BEFORE UPDATE OF status ON walrus.table_reload
FOR EACH ROW EXECUTE FUNCTION walrus.guard_reload_v2_completion();

CREATE TABLE walrus.table_reload_export_range (
  reload_id            bigint NOT NULL
                       REFERENCES walrus.table_reload(reload_id) ON DELETE CASCADE,
  exporter_generation  bigint NOT NULL CHECK (exporter_generation > 0),
  range_no             bigint NOT NULL CHECK (range_no >= 0),
  full_scan            boolean NOT NULL,
  start_block          bigint,
  end_block            bigint,
  status               text NOT NULL DEFAULT 'planned'
                       CHECK (status IN ('planned', 'complete')),
  file_count           bigint,
  row_count            bigint,
  planned_at           timestamptz NOT NULL DEFAULT now(),
  completed_at         timestamptz,
  PRIMARY KEY (reload_id, range_no),
  CHECK (
    (full_scan AND start_block IS NULL AND end_block IS NULL)
    OR
    (NOT full_scan
      AND start_block IS NOT NULL AND start_block >= 0
      AND (end_block IS NULL OR end_block > start_block))
  ),
  CHECK (
    (status = 'planned' AND file_count IS NULL AND row_count IS NULL AND completed_at IS NULL)
    OR
    (status = 'complete'
      AND file_count IS NOT NULL AND file_count >= 0
      AND row_count IS NOT NULL AND row_count >= 0
      AND completed_at IS NOT NULL)
  )
);

CREATE INDEX table_reload_export_range_generation_idx
  ON walrus.table_reload_export_range (reload_id, exporter_generation, status, range_no);

-- A queued source request must not enter `exporting` while the loader is reconciling or recovering
-- publication of its predecessor. Terminal rows leave this fence as before.
DROP INDEX walrus.table_reload_one_live;
CREATE UNIQUE INDEX table_reload_one_live
  ON walrus.table_reload (epoch, source_schema, source_table)
  WHERE status IN ('exporting', 'export_complete', 'publishing')
     OR (status = 'requested' AND source_request_id IS NULL);

-- This is a deliberately coordinated upgrade. Older producers did not persist an object digest,
-- and inventing one would turn corruption detection into theatre. Refuse to migrate a non-empty
-- work queue; operators must drain it with the old binaries before deploying protocol v2.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM walrus.file_manifest) THEN
    RAISE EXCEPTION
      'protocol-v2 migration requires walrus.file_manifest to be empty; drain the old queue first';
  END IF;
  IF EXISTS (
    SELECT 1 FROM walrus.table_reload
    WHERE status NOT IN ('complete', 'failed')
  ) THEN
    RAISE EXCEPTION
      'protocol-v2 migration requires every pre-upgrade reload to be complete or failed';
  END IF;
END
$$;

ALTER TABLE walrus.file_manifest
  ADD COLUMN object_size bigint NOT NULL,
  ADD COLUMN sha256 bytea NOT NULL,
  ADD COLUMN stream_group_id bigint,
  ADD COLUMN stream_group_ordinal bigint,
  ADD CONSTRAINT file_manifest_kind_check
    CHECK (kind IN ('snapshot', 'stream', 'spill', 'reload')),
  ADD CONSTRAINT file_manifest_status_check
    CHECK (status IN ('ready', 'failed')),
  ADD CONSTRAINT file_manifest_row_count_check CHECK (row_count > 0),
  ADD CONSTRAINT file_manifest_object_size_check CHECK (object_size > 0),
  ADD CONSTRAINT file_manifest_sha256_check CHECK (octet_length(sha256) = 32),
  ADD CONSTRAINT file_manifest_lsn_range_check CHECK (lsn_start <= lsn_end),
  ADD CONSTRAINT file_manifest_reload_identity_check
    CHECK ((kind = 'reload') = (reload_id IS NOT NULL)),
  ADD CONSTRAINT file_manifest_stream_group_shape_check
    CHECK (
      (stream_group_id IS NULL AND stream_group_ordinal IS NULL)
      OR
      (
        stream_group_id IS NOT NULL
        AND stream_group_ordinal IS NOT NULL
        AND stream_group_ordinal >= 0
        AND kind IN ('stream', 'spill')
        AND reload_id IS NULL
      )
    ),
  ADD CONSTRAINT file_manifest_s3_uri_unique UNIQUE (s3_uri),
  ADD CONSTRAINT file_manifest_group_ordinal_unique
    UNIQUE (stream_group_id, stream_group_ordinal);

-- A reload object is meaningful only for the exact table/epoch attempt that exported it. A
-- reload_id-only reference would allow an incorrectly labelled object to pass the export seal and
-- later be routed into another table. Ordinary stream rows keep reload_id NULL and therefore do
-- not participate in this composite FK.
ALTER TABLE walrus.table_reload
  ADD CONSTRAINT table_reload_manifest_identity_unique
    UNIQUE (reload_id, epoch, source_schema, source_table);

ALTER TABLE walrus.file_manifest
  ADD CONSTRAINT file_manifest_reload_identity_fk
    FOREIGN KEY (reload_id, epoch, source_schema, source_table)
    REFERENCES walrus.table_reload (reload_id, epoch, source_schema, source_table)
    ON DELETE RESTRICT;

-- Cross-row timing cannot be expressed as a CHECK/FK: reload objects may be inserted only while
-- the exact attempt's exported snapshot is live and unsealed. Taking a key-share lock lets
-- parallel workers update their completed-file count without deadlocking one another. The seal
-- takes an explicit FOR UPDATE lock in a first statement, then validates manifests under the next
-- READ COMMITTED snapshot, so it both waits for these inserts and prevents later ones.
-- The trigger serializes the insert with fail/seal/complete, closing the otherwise possible
-- "late object after seal" window. Status-only integrity updates do not invoke this identity
-- trigger.
CREATE FUNCTION walrus.guard_reload_manifest_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.reload_id IS NULL THEN
    RETURN NEW;
  END IF;

  PERFORM 1
  FROM walrus.table_reload AS reload
  WHERE reload.reload_id = NEW.reload_id
    AND reload.epoch = NEW.epoch
    AND reload.source_schema = NEW.source_schema
    AND reload.source_table = NEW.source_table
    AND reload.status = 'exporting'
    AND reload.start_lsn = NEW.lsn_start
    AND NEW.lsn_end = NEW.lsn_start
    AND reload.schema_version = NEW.schema_version
    AND reload.export_snapshot IS NOT NULL
    AND reload.export_sealed_at IS NULL
  FOR KEY SHARE;

  IF NOT FOUND THEN
    RAISE EXCEPTION
      'reload manifest does not belong to a live, planned, unsealed export attempt'
      USING ERRCODE = '23514', CONSTRAINT = 'file_manifest_reload_attempt_guard';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER file_manifest_reload_attempt_guard
BEFORE INSERT OR UPDATE OF reload_id, epoch, source_schema, source_table,
                           kind, lsn_start, lsn_end, schema_version
ON walrus.file_manifest
FOR EACH ROW EXECUTE FUNCTION walrus.guard_reload_manifest_identity();

-- Old loaders embed an unconditional `DELETE FROM file_manifest WHERE id = ANY(...)`. Make that
-- statement fail after the coordinated upgrade. Every v2 retirement path opts in transaction-
-- locally; this is a rollout compatibility fence, not an authorization boundary.
CREATE FUNCTION walrus.guard_manifest_delete_protocol_v2()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF current_setting('walrus.manifest_delete_protocol', true) IS DISTINCT FROM '2' THEN
    RAISE EXCEPTION
      'file manifests may be retired only by the protocol-v2 grouped/fenced path'
      USING ERRCODE = '23514', CONSTRAINT = 'file_manifest_delete_protocol_v2';
  END IF;
  RETURN OLD;
END
$$;

CREATE TRIGGER file_manifest_delete_protocol_v2
BEFORE DELETE ON walrus.file_manifest
FOR EACH ROW EXECUTE FUNCTION walrus.guard_manifest_delete_protocol_v2();

-- One durable receipt for each protocol-v2 streamed transaction. Keeping the receipt after its
-- child manifests have been consumed makes replay after "publish committed, source ACK lost"
-- idempotent for the lifetime of the slot epoch.
CREATE TABLE walrus.stream_txn_publication (
  id          bigserial PRIMARY KEY,
  epoch       bigint NOT NULL,
  top_xid     bigint NOT NULL CHECK (top_xid BETWEEN 0 AND 4294967295),
  commit_lsn  pg_lsn NOT NULL,
  commit_ts   text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  -- A PostgreSQL commit record has one top-level xid. Keying the receipt by the WAL identity makes
  -- a replay that changes top_xid a semantic conflict instead of a second accepted publication.
  UNIQUE (epoch, commit_lsn)
);

-- A streamed transaction can produce several Parquet objects for one table. The loader must
-- append that complete per-table set in one DuckLake transaction; an arbitrary claim LIMIT may
-- never expose only a prefix of it.
CREATE TABLE walrus.stream_manifest_group (
  id              bigserial PRIMARY KEY,
  publication_id  bigint NOT NULL
                  REFERENCES walrus.stream_txn_publication(id) ON DELETE RESTRICT,
  epoch           bigint NOT NULL,
  top_xid         bigint NOT NULL CHECK (top_xid BETWEEN 0 AND 4294967295),
  source_schema   text NOT NULL,
  source_table    text NOT NULL,
  commit_lsn      pg_lsn NOT NULL,
  commit_ts       text NOT NULL,
  expected_files  bigint NOT NULL CHECK (expected_files > 0),
  row_count       bigint NOT NULL CHECK (row_count > 0),
  -- Stable semantic identity retained after randomized objects/child queue rows are gone. The
  -- producer sorts this array and excludes s3_uri/object bytes deliberately: replay must prove the
  -- same logical files, not reproduce a random object key.
  file_shape      jsonb NOT NULL CHECK (jsonb_typeof(file_shape) = 'array'),
  status          text NOT NULL DEFAULT 'ready'
                  CHECK (status IN ('ready', 'applied', 'failed', 'superseded')),
  created_at      timestamptz NOT NULL DEFAULT now(),
  applied_at      timestamptz,
  UNIQUE (publication_id, source_schema, source_table),
  UNIQUE (id, epoch, source_schema, source_table, commit_lsn),
  CHECK ((status IN ('applied', 'superseded')) = (applied_at IS NOT NULL))
);

ALTER TABLE walrus.file_manifest
  ADD CONSTRAINT file_manifest_stream_group_fk
  FOREIGN KEY (stream_group_id, epoch, source_schema, source_table, lsn_end)
  REFERENCES walrus.stream_manifest_group
    (id, epoch, source_schema, source_table, commit_lsn)
  ON DELETE RESTRICT;

CREATE INDEX stream_manifest_group_ready_idx
  ON walrus.stream_manifest_group (epoch, source_schema, source_table, commit_lsn, id)
  WHERE status = 'ready';

-- Object corruption is a table-level recovery state, not a poison-file skip. The first bounded
-- incident schedules a fresh full-table reconciliation; another incident before that generation
-- publishes is terminal quarantine. Keeping the row after recovery makes the last outcome visible
-- while allowing a later, independent incident to begin a fresh bounded cycle.
CREATE TABLE walrus.table_integrity_recovery (
  epoch               bigint NOT NULL,
  source_schema       text NOT NULL,
  source_table        text NOT NULL,
  status              text NOT NULL
                      CHECK (status IN ('retrying', 'quarantined', 'recovered')),
  attempt_count       int NOT NULL CHECK (attempt_count > 0),
  max_attempts        int NOT NULL CHECK (max_attempts >= 0),
  recovery_reload_id  bigint REFERENCES walrus.table_reload(reload_id) ON DELETE SET NULL,
  failed_manifest_id  bigint NOT NULL,
  failed_group_id     bigint,
  last_error          text NOT NULL,
  first_failed_at     timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (epoch, source_schema, source_table),
  CHECK (status <> 'retrying' OR recovery_reload_id IS NOT NULL)
);

CREATE INDEX table_integrity_recovery_active_idx
  ON walrus.table_integrity_recovery (epoch, status, source_schema, source_table)
  WHERE status IN ('retrying', 'quarantined');
