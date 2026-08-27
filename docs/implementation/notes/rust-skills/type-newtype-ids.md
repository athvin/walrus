# pgoutput wire scalars stay bare integers

> **Status:** evaluated — applied to the domain ids, deferred for the protocol scalars.

## What the rule asks for

`type-newtype-ids` says to wrap ids in newtypes so a `user_id` cannot be passed where a `post_id`
is expected. Walrus already follows it for everything the control plane hands around: `Lsn`
(`crates/common/src/lsn.rs`) plus `ManifestId`, `EpochNo`, `SchemaVersionNo`, `ReloadId`, and
`DdlId` in `crates/common/src/ids.rs`, each a `#[repr(transparent)]` newtype with a hand-written
SQLx `int8` delegation so a `bigint` column round-trips with no SQL cast.

## What the audit changed

- **`DdlId`** — `ddl_manifest.id` was the last control-plane row primary key still typed `i64`
  (`crates/control/src/ddl_manifest.rs`). It is an ordering key, not just a row handle: the DDL
  history is read in `(c_lsn, id)` order, and the row it lives on also carries a `SchemaVersionNo`,
  so a bare `i64` there was one assignment away from the exact confusion the rule describes.
- **`pg_sink::memory::TableId`** — was `pub type TableId = u32`, an alias that *names* an id
  without making it one. Every in-flight stream is metered under `(TableId, xid)`, two `u32`s an
  alias lets a caller transpose silently; a swapped key meters the wrong stream and the accounting
  cannot notice. It is now a newtype, which also types `StreamDemux`'s owner index and
  `StreamedTxn::take_stream(oid, sub_xid)`.

## What deliberately stays a bare integer

The pgoutput wire scalars: relation OIDs (`PgRelation::oid`), type OIDs (`PgColumn::type_oid` and
the whole `common::oids` constant table), and transaction ids (`xid` / `sub_xid` / `top_xid`,
including `SinkMeta::xid`).

They are rule-relevant — a relation OID and a type OID are both `u32` and genuinely confusable —
but converting them is a whole-workspace refactor rather than a focused improvement:

- Ripgrep counts roughly 400 textual `oid`-family occurrences across 73 files and 375 `xid`-family
  occurrences across 38 files (comments included).
- Both cross persisted wire contracts: `type_oid` is part of the `PgRelation` snapshot stored in
  `schema_registry.columns`, and `xid` is a key in the `walrus_pg_sink_meta` provenance document
  (`crates/common/src/sink_meta.rs`). `#[serde(transparent)]` would preserve the JSON, but the
  amortized `MetaConst`/`MetaRow` split has to be changed in lockstep with it.
- The `common::oids` constants are consumed as `match` patterns and range comparisons across
  `pg-to-arrow` and `loader`, and as bare integer literals through per-test `col(name, oid, …)`
  helpers in ~30 test files. Converting the constants and leaving the helpers (or the reverse)
  breaks one side or the other; there is no small consistent slice.

Naming already carries the distinction at every site (`type_oid` vs `oid` vs `relation_oid`), and
the fields are reached through struct literals and named bindings rather than positional argument
lists, so the residual swap risk is much lower than the raw type would suggest.

## What would reverse this

Take the conversion as its own change if any of these appear: a function that takes a relation OID
and a type OID (or two different xids) as adjacent positional `u32` parameters; a second
`u32`-keyed catalog cache alongside `RelationCache`; or a real mix-up found in review or
production. At that point convert `common::oids` to `TypeOid` and `PgRelation::oid` to
`RelationOid` together — the constant table and the struct field are the same edit — and let the
test helpers keep taking `u32` so their integer-literal call sites stay untouched.
