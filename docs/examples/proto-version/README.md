# proto_version / pgoutput — reproducible harness

The Docker harness behind [`docs/proto-version.md`](../../proto-version.md). It stands up a
Postgres 16 with logical replication enabled and a deliberately tiny
`logical_decoding_work_mem` (64kB) so that a few-thousand-row transaction is enough to trip
streaming. Every capture in the write-up was produced by this harness — nothing is invented.

## Quickstart

```bash
docker compose up --wait                                     # Postgres 16, healthy
docker exec -i walrus-proto-pg psql -U postgres -d walrus < 01-setup.sql   # schema + pub + slots
./03-capture.sh                                              # reproduce every capture, labeled
docker compose down -v                                       # tear down
```

## Files

| file | what it is |
|---|---|
| `docker-compose.yml` | Postgres 16, `wal_level=logical`, `logical_decoding_work_mem=64kB` |
| `01-setup.sql`       | tables (single PK / composite PK / `REPLICA IDENTITY FULL`), a custom enum type, `pub`, and both slots (`slot_test` = test_decoding, `slot_pg` = pgoutput) |
| `02-commands.sql`    | copy-paste cheatsheet: capture queries + the command matrix, for poking by hand |
| `03-capture.sh`      | authoritative driver — runs every scenario and shows it in both plugins |
| `decode_pgoutput.py` | stdlib-only pgoutput binary → structured/readable decoder (NOT production code) |
| `test_decode_pgoutput.py` | golden-vector unit tests for the decoder (pure; `python3 -m unittest test_decode_pgoutput`) |
| `run-tests.sh`       | live-Postgres assertion suite for wire behavior (needs the container) |
| `TESTING.md`         | coverage matrix mapping the edge-case catalog to tests / docs / deferred |

## Tests

```bash
python3 -m unittest test_decode_pgoutput -v    # 24 decoder golden-vector tests (no Docker)
./run-tests.sh                                 # 28 live-Postgres wire-behavior assertions
```

See [`TESTING.md`](./TESTING.md) for the full coverage matrix and the honest list of deferred cases.

## The two lenses

Same WAL, two output plugins:

- **`slot_test` (`test_decoding`)** — human-readable text. The Rosetta Stone.
- **`slot_pg` (`pgoutput`)** — the binary format walrus actually decodes. Hex-encode it in SQL
  (`encode(data,'hex')`) and pipe through `decode_pgoutput.py`.

```bash
# pgoutput binary, decoded:
docker exec -i walrus-proto-pg psql -U postgres -d walrus -At -c \
 "SELECT encode(data,'hex') FROM pg_logical_slot_peek_binary_changes('slot_pg',NULL,NULL,
   'proto_version','2','publication_names','pub','streaming','on')" | python3 decode_pgoutput.py

# live walsender path (raw concatenated stream):
docker exec -i walrus-proto-pg pg_recvlogical -U postgres -d walrus --slot=slot_live --start \
  -o proto_version=2 -o publication_names=pub -o streaming=on -f - | python3 decode_pgoutput.py --stream
```

## Notes

- Use **peek** (`pg_logical_slot_peek_*`) while experimenting — it does not consume, so re-runs
  show the same data. **get** (`pg_logical_slot_get_*`) advances the slot.
- An un-consumed slot pins WAL (`restart_lsn` stops moving). `docker compose down -v` throws the
  whole volume away, so nothing leaks between runs.
