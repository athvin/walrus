-- 0003_table_ownership.sql — the loader's cooperative single-writer lease (loader §8.1, PR 3.1).
--
-- The sink gets single-writer for free (Postgres' active-slot rule). The loader must ASSEMBLE it: this
-- control-plane ownership lease is the FIRST fence (a monotonic `fencing_token` per owned table),
-- acquired BEFORE the DuckDB read-write file lock (the second fence). One row per owned
-- (epoch, schema, table). The token is inert while `replicas=1` (persist it now; sharded in PR 4.11).
CREATE TABLE walrus.table_ownership (
  epoch          bigint      NOT NULL,
  source_schema  text        NOT NULL,
  source_table   text        NOT NULL,
  owner_pod      text        NOT NULL,   -- the pod holding the lease
  fencing_token  bigint      NOT NULL DEFAULT 1,  -- monotonic; bumps only on a change of owner
  lease_expiry   timestamptz NOT NULL,   -- a live owner keeps this in the future via renew
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (epoch, source_schema, source_table)
);
