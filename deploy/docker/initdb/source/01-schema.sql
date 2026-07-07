-- 01-schema.sql — source-PG schema + publication for the walrus dev harness.
--
-- Runs once via /docker-entrypoint-initdb.d on first cluster init (against POSTGRES_DB=walrus).
-- Adapted from docs/examples/proto-version/01-setup.sql.
--
-- NOTE: no replication slots are created here. The sink creates and owns walrus's lifelong
-- logical slot at bootstrap (Phase 2), capturing the exported snapshot. A slot made here would
-- be orphaned and pin WAL forever. Publication-only is correct for this harness.

-- ---------------------------------------------------------------------------
-- Schema.
--   orders    : single-column PK, REPLICA IDENTITY DEFAULT (the PK). The common case.
--               `feeling` is a CUSTOM enum type -> forces a pgoutput `Type` ('Y') message.
--   customers : COMPOSITE PK (region, id) -> multi-column keys in Relation + tuples.
--   items     : REPLICA IDENTITY FULL -> UPDATE/DELETE carry the WHOLE old row ('O'), not
--               just the key ('K').
-- ---------------------------------------------------------------------------
CREATE TYPE mood AS ENUM ('happy', 'meh', 'sad');

CREATE TABLE public.orders (
    id      int         PRIMARY KEY,
    status  text        NOT NULL,
    amount  numeric(10,2),
    feeling mood,
    note    text
);  -- REPLICA IDENTITY DEFAULT is implicit (uses the PK)

CREATE TABLE public.customers (
    region  text,
    id      int,
    name    text,
    PRIMARY KEY (region, id)
);

CREATE TABLE public.items (
    id    int PRIMARY KEY,
    label text,
    qty   int
);
ALTER TABLE public.items REPLICA IDENTITY FULL;

-- ---------------------------------------------------------------------------
-- Publication. pgoutput only decodes tables in the publication named at
-- START_REPLICATION time; publish everything. (The sink's own ddl_audit / heartbeat
-- tables are added to a real publication in Phase 2 — not needed for this harness.)
-- ---------------------------------------------------------------------------
CREATE PUBLICATION walrus_pub FOR ALL TABLES;
