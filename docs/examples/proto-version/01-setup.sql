-- 01-setup.sql — schema, publication, and both replication slots.
-- Idempotent: safe to re-run (drops slots/publication/tables first).
-- Run with:  docker exec -i walrus-proto-pg psql -U postgres -d walrus < 01-setup.sql

-- ---------------------------------------------------------------------------
-- Clean slate (slots must be inactive; stop any pg_recvlogical first).
-- ---------------------------------------------------------------------------
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name IN ('slot_test', 'slot_pg');

DROP PUBLICATION IF EXISTS pub;
DROP TABLE IF EXISTS public.orders, public.customers, public.items CASCADE;
DROP TYPE IF EXISTS mood;

-- ---------------------------------------------------------------------------
-- Schema.
--   orders    : single-column PK, REPLICA IDENTITY DEFAULT (the PK). The common case.
--               `mood` is a CUSTOM enum type -> forces a pgoutput `Type` ('Y') message.
--   customers : COMPOSITE PK (region, id) -> shows multi-column keys in Relation + tuples.
--   items     : REPLICA IDENTITY FULL -> UPDATE/DELETE carry the WHOLE old row ('O'),
--               not just the key ('K'). This is the contrast the doc needs.
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
-- Publication + slots.
--   pgoutput only decodes tables that belong to the publication named at
--   START_REPLICATION time, so publish everything.
-- ---------------------------------------------------------------------------
CREATE PUBLICATION pub FOR ALL TABLES;

-- Same WAL, two lenses:
--   slot_test = test_decoding (human-readable text)
--   slot_pg   = pgoutput      (binary; the format walrus actually decodes)
SELECT 'created test_decoding slot' AS step,
       slot_name, lsn
FROM pg_create_logical_replication_slot('slot_test', 'test_decoding');

SELECT 'created pgoutput slot' AS step,
       slot_name, lsn
FROM pg_create_logical_replication_slot('slot_pg', 'pgoutput');

SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots ORDER BY slot_name;
