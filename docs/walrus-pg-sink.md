# `walrus-pg-sink` — the WAL consumer, in depth (type conversion · DDL capture · pod lifecycle)

> **Status: design deep-dive, companion to [architecture.md](./architecture.md).** Where
> `architecture.md` sketches the whole system, this doc is the **authoritative specification for the
> three areas the sink lives or dies on** — the ones the master sketch either flags as unverified,
> leaves underspecified, or gets wrong:
>
> 1. **Data-type conversion** (Postgres → Arrow → Parquet → DuckDB), end to end, per type, with the
>    exact multi-column **decomposition** for every type that is *not* one-to-one.
> 2. **DDL capture** — the event-trigger tap on the source database, since logical decoding never
>    emits DDL.
> 3. **Kubernetes pod lifecycle** — startup, steady-state, and (the piece missing everywhere else)
>    **graceful shutdown**, so a pod can spin up and down and it *just works*.
>
> Every non-obvious claim here was checked against primary sources (PostgreSQL / AWS DMS / DuckDB /
> Apache Parquet + Arrow / Kubernetes manuals) in an adversarial deep-research pass; inline `[n]`
> markers reference the **[Sources](#sources)** section. This doc **supersedes** the corresponding
> rows in `architecture.md` — see [§5](#5-what-this-doc-supersedes-in-architecturemd).

---

## Table of contents

- [1. Mission recap](#1-mission-recap)
- [2. Data-type conversion (Postgres → Arrow → Parquet → DuckDB)](#2-data-type-conversion-postgres--arrow--parquet--duckdb)
  - [2.1 The one rule that governs everything](#21-the-one-rule-that-governs-everything-duckdb-reads-parquet-native-types)
  - [2.2 The three-tier model](#22-the-three-tier-model)
  - [2.3 The full type table](#23-the-full-type-table)
  - [2.4 Tier-2 decompositions, column by column](#24-tier-2-decompositions-column-by-column)
  - [2.5 Tier-3 canonical-text carriers](#25-tier-3-canonical-text-carriers)
  - [2.6 The type-mapping descriptor](#26-the-type-mapping-descriptor-how-the-loader-rebuilds-exactly)
  - [2.7 Special values, NULLs, and the gotchas we inherit](#27-special-values-nulls-and-the-gotchas-we-inherit)
  - [2.8 Round-trip conformance tests](#28-round-trip-conformance-tests-the-seams-that-must-be-proven)
- [3. DDL capture — the sink's tap on the source](#3-ddl-capture--the-sinks-tap-on-the-source)
  - [3.1 Why a trigger is the only way](#31-why-a-trigger-is-the-only-way)
  - [3.2 The AWS DMS pattern, and where we deviate](#32-the-aws-dms-pattern-and-where-we-deviate)
  - [3.3 The audit table](#33-the-audit-table)
  - [3.4 The two event triggers](#34-the-two-event-triggers)
  - [3.5 How the sink consumes it](#35-how-the-sink-consumes-it)
  - [3.6 Limitations & backstops](#36-limitations--backstops)
  - [3.7 Preflight](#37-preflight)
- [4. Kubernetes pod lifecycle](#4-kubernetes-pod-lifecycle)
  - [4.1 Topology & the real correctness backstop](#41-topology--the-real-correctness-backstop)
  - [4.2 Startup](#42-startup)
  - [4.3 Probes — get these exactly right](#43-probes--get-these-exactly-right)
  - [4.4 Steady state](#44-steady-state)
  - [4.5 Graceful shutdown — the missing piece](#45-graceful-shutdown--the-missing-piece)
  - [4.6 The loader's shutdown differs](#46-the-loaders-shutdown-differs)
  - [4.7 Decommission — the only place the slot is dropped](#47-decommission--the-only-place-the-slot-is-dropped)
- [5. What this doc supersedes in architecture.md](#5-what-this-doc-supersedes-in-architecturemd)
- [Sources](#sources)

---

## 1. Mission recap

`walrus-pg-sink` has one job (spelled out in [architecture.md §1](./architecture.md#component-1--postgres-sink-walrus-pg-sink)):
**drain the WAL to durable storage, fast and safely** — consume the pgoutput stream (`proto_version '2'`
+ `streaming 'on'`), convert each change row Postgres → Arrow → Parquet, PUT it to S3, record a
manifest row, and advance `confirmed_flush_lsn` **only after** that write is durable so the slot can
never run away. It does **not** reconcile or model data into its final shape — that is `walrus-loader`,
which reads the Parquet into DuckDB, appends verbatim to a `<table>_raw` CDC log, and `MERGE`s into a
`<table>` mirror that must match the **exact current shape of the source Postgres table**.

That last clause — *exact current shape* — is why this doc exists. Two things stand between a change on
the source and a faithful mirror in DuckDB, and both are the sink's responsibility to get right:

- **The type on the wire must survive four hops** (Postgres → Arrow → Parquet → DuckDB) with enough
  fidelity that the loader can rebuild the source value. Most types survive intact; a stubborn minority
  do not, and for those the sink must **emit more than one column** so nothing is lost ([§2](#2-data-type-conversion-postgres--arrow--parquet--duckdb)).
- **The schema itself changes over time**, and logical decoding never tells us — so the sink taps the
  source with an event trigger and lets those changes ride the same slot inline ([§3](#3-ddl-capture--the-sinks-tap-on-the-source)).

And because all of this runs on Kubernetes, the process wrapped around it must **start, run, and stop**
without ever corrupting the slot or losing an in-flight batch ([§4](#4-kubernetes-pod-lifecycle)).

---

## 2. Data-type conversion (Postgres → Arrow → Parquet → DuckDB)

> **The honest headline: it is *not* one-to-one.** Most scalar types round-trip cleanly, but Postgres
> has a far richer type system than Parquet or DuckDB, and the gap is exactly where the "reconcile to
> source truth" mission is won or lost. The `architecture.md` type table
> ([its own words](./architecture.md#data-type-translation-postgres--arrow--parquet)) called this *"the
> design's biggest unknown… a starting point to validate in a spike, not gospel."* This section is that
> validation.

Apache Arrow is the single intermediate representation: the sink decodes a pgoutput tuple into typed
Rust values, builds an Arrow `RecordBatch`, and writes it as Parquet via arrow-rs; the loader reads that
Parquet into DuckDB with `read_parquet`. Fidelity is therefore bounded by the **narrowest** of those
four type systems on any given path.

### 2.1 The one rule that governs everything: DuckDB reads Parquet-native types

**DuckDB infers column types from the Parquet file's *native logical types*, not from arrow-rs's
`ARROW:schema` extension metadata** [12][13][18]. arrow-rs embeds a serialized Arrow schema in the
Parquet key-value metadata, but DuckDB's `read_parquet` ignores it and reads the standard Parquet
`LogicalType` annotations. Consequence: **every distinction we need on the DuckDB side must be expressed
as a real Parquet logical type**, or it is silently lost:

- a UUID that is a bare `FixedSizeBinary(16)` reads back as **BLOB**, not `UUID` ([§2.4](#uuid));
- a `timestamptz` vs `timestamp` distinction is the Parquet `isAdjustedToUTC` flag, nothing else;
- a `DECIMAL(p,s)` must carry the Parquet DECIMAL annotation or it reads as raw bytes/ints.

Two corollaries we bake in everywhere:

- **Use `MICROS` for *all* temporal types — never `MILLIS`, never `NANOS`.** Postgres time resolution is
  microseconds; `NANOS` combined with a timezone silently downgrades in DuckDB (there is no
  `TIMESTAMP_NS WITH TIME ZONE`) and overflows `int64` at extreme dates, while `MILLIS` truncates [12][14][19].
- **arrow-rs's writer, not DuckDB's, defines the bytes.** Where DuckDB's own Parquet *writer* has
  historically used non-standard encodings (e.g. `TIME`), we write standard Parquet logical types from
  arrow-rs and prove the read side with a conformance test ([§2.8](#28-round-trip-conformance-tests-the-seams-that-must-be-proven)).

### 2.2 The three-tier model

Every source column is classified into one of three tiers. The tier is **recorded per column in the
schema registry / manifest** ([§2.6](#26-the-type-mapping-descriptor-how-the-loader-rebuilds-exactly))
so the loader rebuilds deterministically.

| Tier | Meaning | Examples |
|---|---|---|
| **Tier 1 — native 1:1** | The Parquet-native logical type survives into DuckDB unchanged; one source column → one mirror column, lossless. | `bool`, `int2/4/8`, `float4/8`, `numeric(p,s)` with `p≤38`, `bytea`, `char/varchar/text` (content), `date`, `time`, `timestamp`, `timestamptz`, `json`, arrays, composite, `hstore` |
| **Tier 2 — structural decomposition** | Carries more information than any single Arrow/Parquet/DuckDB scalar can hold → the sink emits **multiple columns** or a **nested** type the loader recombines. | `range` (5 cols), `multirange` (`LIST<STRUCT>`), `interval` (3 ints), `timetz` (2 cols), native geometric (`STRUCT`/`LIST` of doubles) |
| **Tier 3 — canonical-text carrier** | No lossless structural target → carry as canonical **VARCHAR**, cast on load, and re-apply lost metadata from the descriptor. | unconstrained / `>38`-digit `numeric`, `bit`/`varbit`, `enum`, `inet`/`cidr`/`macaddr(8)`, `tsvector`/`tsquery`, `pg_lsn`, `xid`/`xid8`/`txid_snapshot`, PostGIS |

### 2.3 The full type table

`<c>` = the source column name; suffixed columns (`<c>_lower`, …) are what the sink actually emits.

| Tier | Postgres | Arrow `DataType` | Parquet logical | DuckDB target | 1:1? | Emitted column(s) |
|---|---|---|---|---|---|---|
| 1 | `boolean` | `Boolean` | BOOLEAN (none) | `BOOLEAN` | ✅ | `<c> BOOLEAN` |
| 1 | `smallint` | `Int16` | INT(16,true) | `SMALLINT` | ✅ | `<c> SMALLINT` *(may widen to INTEGER; still lossless)* |
| 1 | `integer` | `Int32` | INT32 | `INTEGER` | ✅ | `<c> INTEGER` |
| 1 | `bigint` | `Int64` | INT64 | `BIGINT` | ✅ | `<c> BIGINT` |
| 1 | `real` | `Float32` | FLOAT | `FLOAT` | ✅ | `<c> FLOAT` |
| 1 | `double precision` | `Float64` | DOUBLE | `DOUBLE` | ✅ | `<c> DOUBLE` |
| 1 | `numeric(p,s)`, **p≤38** | `Decimal128(p,s)` | DECIMAL | `DECIMAL(p,s)` | ✅ | `<c> DECIMAL(p,s)` |
| **3** | `numeric` **unconstrained** | `Utf8` | STRING | `VARCHAR` | ❌ | `<c> VARCHAR` |
| **3** | `numeric(p,s)`, **p>38** | `Utf8` | STRING | `VARCHAR` | ❌ | `<c> VARCHAR` |
| **3** | `money` | `Decimal128(19,2)` | DECIMAL | `DECIMAL(19,2)` | ⚠ | `<c> DECIMAL(19,2)` *(fraction digits = `lc_monetary`, carried as metadata)* |
| 1 | `char(n)`/`varchar(n)`/`text` | `Utf8` (`LargeUtf8` if huge) | STRING | `VARCHAR` | ⚠ | `<c> VARCHAR` *(length + bpchar padding carried as metadata)* |
| 1 | `bytea` | `Binary`/`LargeBinary` | BYTE_ARRAY (none) | `BLOB` | ✅ | `<c> BLOB` |
| **3** | `uuid` | `FixedSizeBinary(16)` **+ `arrow.uuid`** | UUID | `UUID` | ⚠ | `<c> UUID` *(native only with the extension; else VARCHAR + CAST)* |
| **3** | `bit`/`bit varying` | `Utf8` (`'0'/'1'`) | STRING | `BIT` (via CAST) | ❌ | `<c> VARCHAR` *(bit length carried as metadata)* |
| **3** | `enum` | `Utf8` | STRING | `ENUM` (via CAST) | ❌ | `<c> VARCHAR` *(ordered label set carried as metadata)* |
| 1 | `date` | `Date32` | DATE | `DATE` | ✅ | `<c> DATE` |
| 1 | `time` | `Time64(µs)` | TIME(MICROS, utc=false) | `TIME` | ✅ | `<c> TIME` |
| **2** | `time with time zone` (`timetz`) | `Int64` + `Int32` | INT64 + INT32 | `TIMETZ` (rebuilt) | ❌ | `<c>_micros BIGINT`, `<c>_offset_seconds INTEGER` |
| 1 | `timestamp` | `Timestamp(µs, None)` | TIMESTAMP(MICROS, utc=false) | `TIMESTAMP` | ✅ | `<c> TIMESTAMP` |
| 1 | `timestamptz` | `Timestamp(µs, "UTC")` | TIMESTAMP(MICROS, utc=true) | `TIMESTAMPTZ` | ✅ | `<c> TIMESTAMPTZ` *(normalized UTC)* |
| **2** | `interval` | 3 × int | INT32 + INT32 + INT64 | `INTERVAL` (rebuilt) | ❌ | `<c>_months INTEGER`, `<c>_days INTEGER`, `<c>_micros BIGINT` |
| 1 | `T[]` (array) | `List<T>` | LIST | `LIST(T)` | ⚠ | `<c> LIST(T)` *(nested `LIST` for multi-dim; custom lower bounds lost)* |
| 1 | composite / row | `Struct<…>` | (nested) | `STRUCT(…)` | ✅ | `<c> STRUCT(…)` |
| 1 | `hstore` | `Map<Utf8,Utf8>` | MAP | `MAP(VARCHAR,VARCHAR)` | ✅ | `<c> MAP(VARCHAR,VARCHAR)` *(or JSON text)* |
| 1 | `json` | `Utf8` | STRING/JSON | `JSON` | ✅ | `<c> JSON` *(verbatim, byte-lossless)* |
| 1 | `jsonb` | `Utf8` | STRING/JSON | `JSON` | ⚠ | `<c> JSON` *(re-serialized: normalized, semantically lossless)* |
| **2** | **range** (`int4range`…`tstzrange`) | (5 fields) | (5 cols) | (5 cols) | ❌ | `<c>_lower`, `<c>_upper`, `<c>_lower_inc BOOLEAN`, `<c>_upper_inc BOOLEAN`, `<c>_empty BOOLEAN` |
| **2** | **multirange** (PG14+) | `List<Struct>` | LIST<STRUCT> | `LIST(STRUCT(…))` | ❌ | `<c> LIST(STRUCT(lower,upper,lower_inc,upper_inc))` |
| **2** | geometric `point` | `Struct<x,y>` | STRUCT | `STRUCT(x DOUBLE, y DOUBLE)` | ❌ | `<c> STRUCT(x,y)` |
| **2** | geometric `line`/`lseg`/`box`/`circle` | `Struct` | STRUCT | `STRUCT(…)` | ❌ | see [§2.4](#geometric-types) |
| **2** | geometric `path`/`polygon` | `Struct`/`List` | LIST<STRUCT> | `LIST(STRUCT(x,y))` | ❌ | `path` carries `is_closed`; see [§2.4](#geometric-types) |
| **3** | `inet`/`cidr` | `Utf8` | STRING | `VARCHAR` | ❌ | `<c> VARCHAR` *(canonical)* |
| **3** | `macaddr`/`macaddr8` | `Utf8` | STRING | `VARCHAR` | ❌ | `<c> VARCHAR` |
| **3** | `tsvector`/`tsquery` | `Utf8` | STRING | `VARCHAR` | ❌ | `<c> VARCHAR` *(canonical)* |
| **3** | `pg_lsn` | `Utf8` | STRING | `VARCHAR` | ❌ | `<c> VARCHAR` *(or `UBIGINT`)* |
| **3** | `xid`/`xid8`/`txid_snapshot` | `Utf8` | STRING | `VARCHAR` | ❌ | `<c> VARCHAR` |
| **3** | PostGIS `geometry`/`geography` | `Binary` + `Int32` | BYTE_ARRAY + INT32 | `BLOB`+`INTEGER` | ❌ | `<c>_wkb BLOB`, `<c>_srid INTEGER` *(deferred; see [§2.4](#postgis))* |
| 1 | `domain` | *(base type's mapping)* | *(base)* | *(base)* | ✅ | maps as the **base type**; domain name/constraints are catalog metadata |
| — | `xml` | `Utf8` | STRING | `VARCHAR` | ✅ | `<c> VARCHAR` |

> **The two `numeric` cases must stay distinct in code.** `numeric(p,s)` with `p≤38` is a clean Tier-1
> `DECIMAL(p,s)`. *Unconstrained* `numeric` and any declared `p>38` are Tier-3 `VARCHAR`. Do **not**
> collapse them into one branch — the first is arithmetic-ready and lossless; the second would silently
> overflow or downcast (see [§2.5](#unconstrained--38-digit-numeric)).

### 2.4 Tier-2 decompositions, column by column

These are the types the user's instinct pointed at — *"the interval time range… we will likely need to
create two columns."* Confirmed: they are not one-to-one, and the fix is to emit multiple typed columns
the loader recombines into the native DuckDB type (or keeps as siblings). Chosen shapes:

#### `interval` → three signed-integer columns

Postgres `interval` is deliberately **un-normalized**: it stores `months (int32)`, `days (int32)`, and
`microseconds (int64)` as three independent fields (so `'1 month'` ≠ `'30 days'` ≠ `'720 hours'`) [11].
**DuckDB's `INTERVAL` is the *same* three-field struct** [15], so the clean, byte-identical mapping is a
three-column decomposition the loader recombines:

```
<c>_months  INTEGER   -- source interval months  (signed)
<c>_days    INTEGER   -- source interval days    (signed)
<c>_micros  BIGINT    -- source interval microseconds (signed)
-- loader rebuild:  to_months(<c>_months) + to_days(<c>_days) + to_microseconds(<c>_micros)
-- one shared NULL flag: all three NULL ⇔ the source value was NULL
```

**Rejected alternatives** (all verified lossy):

- `Arrow Interval(MonthDayNano)` — arrow-rs **cannot write it to Parquet**; it *errors* (not truncates)
  [8][20]. This is the mapping `architecture.md` currently proposes, and it does not work.
- Parquet-native `INTERVAL` logical type — 12-byte **unsigned** months/days/**millis**: loses the sign
  and truncates microseconds → milliseconds, with an undefined sort order [10][11].
- a single `int64` of total microseconds (Debezium / DMS `MicroDuration` style) — destroys the
  months/days/micros distinction (months and days are not fixed-length) [16].

> **⚠ Caveat to carry into the transform:** DuckDB normalizes intervals for **equality and ordering**
> (days→24h, months→30d), so two byte-different intervals can compare equal [15]. **Never use an
> interval column in a `MERGE` join key or `PARTITION BY`.** (The PK is always the source primary key,
> so this is a guardrail, not a live risk.)

#### `timetz` → micros + offset

Arrow has **no timezone-aware time type** (only `Timestamp` carries a zone), and Parquet `TIME` has only
the boolean `isAdjustedToUTC`, no per-value offset [12][14]. Decompose:

```
<c>_micros          BIGINT    -- microseconds since midnight (Time64[µs])
<c>_offset_seconds  INTEGER   -- UTC offset in seconds (sign convention pinned by a round-trip test)
-- loader rebuilds a DuckDB TIMETZ.  (Do NOT drop the zone the way AWS DMS does.)
```

#### range → five flat sibling columns

DuckDB has **no range type** [17] (and no `MULTIRANGE`), so a range can never be a single 1:1 mirror
column. **Decision (locked): flat sibling columns** — the shape the user's "two columns" instinct
pointed at, and the easiest to query directly:

```
<c>_lower       <elem>     -- lower(r)       ; NULL if unbounded-below or empty
<c>_upper       <elem>     -- upper(r)       ; NULL if unbounded-above or empty
<c>_lower_inc   BOOLEAN    -- lower_inc(r)
<c>_upper_inc   BOOLEAN    -- upper_inc(r)
<c>_empty       BOOLEAN    -- isempty(r)
```

Element type per family: `int4range → INTEGER`, `int8range → BIGINT`, `numrange → DECIMAL(p,s)` (or
`VARCHAR` if unconstrained, per [§2.5](#unconstrained--38-digit-numeric)), `tsrange → TIMESTAMP`,
`tstzrange → TIMESTAMPTZ`, `daterange → DATE` [17][21]. **Encoding rules (uniform):**

- whole-column SQL `NULL` → **all five columns NULL**;
- **empty** range (`isempty` true) → `_empty = true`, both bounds `NULL`;
- **unbounded** side → that bound `NULL` with `_empty = false` (so `unbounded` is distinguishable from
  `empty`; the `lower_inf`/`upper_inf` predicates are *derivable* = `bound IS NULL AND NOT _empty` and
  are **not** stored);
- discrete subtypes (`int4/int8/date`) are canonicalized by Postgres to `[)` form but still emit all
  five flags **uniformly** (continuous `numrange`/`tsrange`/`tstzrange` preserve arbitrary inclusivity)
  [21].

> Populate directly from `lower()`, `upper()`, `lower_inc()`, `upper_inc()`, `isempty()` — a range is
> **losslessly reconstructable** from exactly these five values [21].

#### multirange → `LIST<STRUCT>`

A multirange (PG14+) is a variable-length, ordered set of **non-empty, non-null** ranges — no flat
representation exists, so it is `LIST<STRUCT>`:

```
<c>  LIST(STRUCT(lower <elem>, upper <elem>, lower_inc BOOLEAN, upper_inc BOOLEAN))
-- member bounds stay NULLABLE (a member may be unbounded); no per-member _empty needed.
-- empty multirange  = empty list (length 0);  SQL NULL = NULL list  — kept DISTINCT.
```

#### geometric types

Native Postgres geometric types are all doubles [23]; carry them as `STRUCT`/`LIST` of doubles so they
stay queryable (Debezium models `point` the same way [16]):

```
point            STRUCT(x DOUBLE, y DOUBLE)
line   (Ax+By+C) STRUCT(a DOUBLE, b DOUBLE, c DOUBLE)
lseg / box       STRUCT(p1 STRUCT(x,y), p2 STRUCT(x,y))
circle           STRUCT(x DOUBLE, y DOUBLE, r DOUBLE)
path             STRUCT(is_closed BOOLEAN, points LIST(STRUCT(x,y)))   -- is_closed is MANDATORY or it's lossy
polygon          LIST(STRUCT(x DOUBLE, y DOUBLE))
```

#### PostGIS

<a id="postgis"></a>**Deferred** unless a source table needs it. DuckDB's core `GEOMETRY` type only
arrived in **v1.5** — the pinned **1.4.x LTS** the loader targets (for `MERGE INTO`) has geometry only
via the Spatial extension [15]. If in scope, carry **WKB + SRID** (WKB, not WKT, to preserve Z/M and
precision; matches Debezium's `STRUCT(srid, wkb)`) and rebuild via `ST_GeomFromWKB`:

```
<c>_wkb   BLOB       -- EWKB/WKB bytes
<c>_srid  INTEGER
```

Revisit a native `GEOMETRY` 1:1 mapping on the next-LTS bump.

#### uuid

<a id="uuid"></a>**Conditionally 1:1.** DuckDB reads a Parquet **UUID logical type** back as native
`UUID`, but arrow-rs only emits that annotation when the `FixedSizeBinary(16)` field carries the
**`arrow.uuid` canonical extension type**; a *plain* FSB(16) is written un-annotated and reads as a
16-byte **BLOB** [12][13]. **Decision:** emit native `UUID` via the extension, **guarded by a CI
`write → read_parquet → typeof == UUID` assertion and a pinned arrow-rs version**; fall back to
`VARCHAR` + `CAST(x AS UUID)` if a pinned release ever drops the annotation on the normal column path.

### 2.5 Tier-3 canonical-text carriers

No lossless structural target exists, so carry the canonical text and let the loader `CAST`. The value
is always lossless as text; what's lost is **type metadata**, which the descriptor
([§2.6](#26-the-type-mapping-descriptor-how-the-loader-rebuilds-exactly)) re-applies.

<a id="unconstrained--38-digit-numeric"></a>**Unconstrained / `>38`-digit `numeric` → `VARCHAR`
(locked decision).** Unconstrained `numeric` is arbitrary precision (up to 131072 digits before the
point, 16383 after) **with a per-row-variable scale** — no single Arrow `Decimal(p,s)` fits it (fixed
scale truncates, out-of-range overflows) [22]. And DuckDB's own ceiling forces the issue: **DuckDB
`DECIMAL` caps at precision 38, and its Parquet reader explicitly downcasts any Parquet decimal with
precision > 38 to `DOUBLE`** (verified in `parquet_reader.cpp`) — so `Decimal256` does **not** round-trip
[9][13]. `VARCHAR` is exact and lossless (matches Debezium string mode and AWS DMS `STRING` [16][6]); the
loader may `CAST` to `DECIMAL(38,s)`/`DOUBLE` only where a column's range is provably safe.

**`bit`/`varbit` → `'0'/'1'` VARCHAR.** DuckDB *has* a native `BIT` type, but neither Arrow nor Parquet
has a BIT logical type. Carry a self-describing `'0'/'1'` string (length is intrinsic) and `CAST(x AS
BIT)` on load — preferred over packed binary, which would need the significant-bit count out-of-band.

**`enum` → `VARCHAR` + ordered label set.** DuckDB has a native `ENUM`, but its Parquet reader maps both
the ConvertedType `ENUM` and `UTF8` to `VARCHAR` — it never reconstructs a native enum [13]. Values are
lossless as strings; the **ordered allowed-label set** is lost on the wire and carried in the descriptor,
from which the loader recreates the DuckDB `ENUM` type and `CAST`s.

**Network / text-search / system types → canonical `VARCHAR`.** `inet`/`cidr`/`macaddr`/`macaddr8`,
`tsvector`/`tsquery`, `pg_lsn`, `xid`/`xid8`/`txid_snapshot` have no structural target; their canonical
text round-trips via the corresponding `::type` cast (AWS DMS marks most of these "does not migrate" —
we do better by keeping the canonical string [6]). `inet`/`cidr` may optionally split into
`_addr`/`_masklen`/`_family` if richer querying is wanted.

### 2.6 The type-mapping descriptor (how the loader rebuilds exactly)

The sink writes, **per source column**, a mapping descriptor into `schema_registry` (keyed by
`schema_version`) and references it from the manifest. It records what Parquet/DuckDB **collapse on read**
so the loader can restore the exact source shape:

```jsonc
{
  "column": "duration",
  "pg_type_oid": 1186, "pg_type": "interval",
  "tier": 2,
  "arrow": "Struct/Decomposed", "duckdb": "INTERVAL",
  "emit": ["duration_months:INT32", "duration_days:INT32", "duration_micros:INT64"],
  "recombine": "to_months(m)+to_days(d)+to_microseconds(us)",
  "meta": {                        // metadata Parquet/DuckDB lose, re-applied by the loader:
    "enum_labels": null,           //   ordered label set for enum
    "bit_length": null,            //   n for bit(n)/varbit(n)
    "char_length": null,           //   n + bpchar padding for char(n)/varchar(n)
    "money_fraction_digits": null  //   lc_monetary fractional digits
  }
}
```

This descriptor is what makes "reconcile to exact source shape" a **mechanical** operation rather than a
guess: the loader reads it, recreates enum types / bit lengths / char lengths / interval structs, and
`CAST`s the carried columns into place.

### 2.7 Special values, NULLs, and the gotchas we inherit

- **NULL vs unchanged-TOAST.** pgoutput distinguishes a real SQL `NULL` (`'n'`) from an *unchanged-TOAST*
  placeholder (`'u'`) [proto-version.md §5](./proto-version.md#5-tupledata-and-the-unchanged-toast-placeholder).
  The Arrow validity bitmap encodes `NULL`; the TOAST sentinel is carried **verbatim** and resolved by
  the loader's back-scan — a *transform* concern, not a type-mapping one
  ([architecture.md §2.1](./architecture.md#21-the-raw-to-mirror-transform-model)).
- **`STORED` generated columns arrive as NULL** over pgoutput [architecture.md](./architecture.md#data-type-translation-postgres--arrow--parquet).
  Detect from the catalog during registry hydration and mark the column derived — do not store a false NULL.
- **Special temporal values — one uniform policy** (decide once, apply everywhere including range
  **bounds**): `infinity`/`-infinity` for `date`/`timestamp`/`timestamptz`, exact `24:00:00` for `time`,
  BC dates. **Recommendation:** use DuckDB's **native infinity** timestamps where supported, and pick an
  explicit null-vs-sentinel for the rest — applied identically to scalars and to range bound values (a
  bound of `infinity` is *distinct* from an unbounded bound, which is `NULL`). Do **not** silently clamp
  the way AWS DMS does (it truncates `infinity` to `9999-12-31` / `4713-01-01 BC`) [6].
- **REPLICA IDENTITY / PK.** Unchanged — a primary key is mandatory and gives `UPDATE`/`DELETE` their key
  columns ([architecture.md §1.1](./architecture.md#11-source-side-setup-one-time-via-migrationjob)).

### 2.8 Round-trip conformance tests (the seams that must be proven)

Because DuckDB reads Parquet-native types ([§2.1](#21-the-one-rule-that-governs-everything-duckdb-reads-parquet-native-types)),
the mapping is only real if a byte written by arrow-rs reads back as the intended DuckDB type. Ship a
**per-type conformance test**: build the Arrow array → write Parquet with arrow-rs → `read_parquet` in an
in-process DuckDB → assert **both** the inferred DuckDB type **and** the value. Focus on the seams:

`smallint` INT(16) annotation · `uuid` UUID annotation (native vs BLOB) · `numeric` p>38 downcast to
DOUBLE · `interval` 3-column rebuild · `timetz` offset sign · range 5-column (empty/unbounded/discrete
canonicalization) · multirange list · `enum` label-set restore · `timestamptz` vs `timestamp`
(`isAdjustedToUTC`) · `bit` length · `money` scale · arrays / composite / hstore nesting · `time`
`24:00:00` edge.

This extends `architecture.md`'s "Types" verification bullet with the specific arrow-rs↔DuckDB assertions.

---

## 3. DDL capture — the sink's tap on the source

> **The problem in one line:** Postgres logical decoding emits only DML — it **never** emits DDL
> [3][5][proto-version.md](./proto-version.md). So if a column is added, dropped, renamed, or retyped
> on the source, the WAL stream says nothing, and the DuckDB mirror silently drifts from source truth.
> The sink must therefore **tap the source database directly** with an event trigger.

### 3.1 Why a trigger is the only way

There is no in-stream signal for schema change, and polling the catalog on a timer can't be ordered
against the data (a poll can't tell you a column was added *between* row 999 and row 1000 of a
transaction). The only mechanism that (a) fires synchronously with the DDL and (b) can be **ordered
inline with the DML** is an **event trigger** that writes an audit row into a **published** table — so
its `INSERT` rides the *same* replication slot as the data, in commit order [24][25].

### 3.2 The AWS DMS pattern, and where we deviate

We adapt the AWS DMS **`awsdms_ddl_audit`** carrier-table pattern [6]: an event-trigger function writes a
row into an audit table that is part of the publication, so the sink sees `"schema changed to X at LSN
L"` in commit order with the data. Three **deliberate deviations**, each verified correct:

| # | AWS DMS does | walrus does | Why |
|---|---|---|---|
| a | `INSERT`s then **`DELETE`s** the audit row in the same txn (it only needs the WAL `INSERT` record) | **Keeps** the row (drops the DELETE) | A durable, replayable schema history; the sink still acts on the decoded `INSERT` op. |
| b | Captures opaque raw `current_query()` **text** only | Snapshots the **structured resulting column set** from the catalog (`pg_attribute`) | The loader derives exact DuckDB DDL from `new − old` — **no Postgres→DuckDB DDL parser** ([architecture.md](./architecture.md#per-change-type-handling-schema-evolution-semantics)). |
| c | Gates on four exact command-tag strings | Captures on `ddl_command_end` for all relation-affecting tags **plus a second `sql_drop` trigger** | Drops (esp. `DROP COLUMN` identity and CASCADE victims) are only reliably enumerable via `sql_drop` ([§3.4](#34-the-two-event-triggers)). |

### 3.3 The audit table

```sql
CREATE SCHEMA IF NOT EXISTS walrus;

CREATE TABLE walrus.ddl_audit (
  c_key         bigserial PRIMARY KEY,
  c_time        timestamptz NOT NULL DEFAULT now(),   -- UTC (our metadata rule; not AWS's bare timestamp)
  c_role        text        NOT NULL DEFAULT current_user,
  c_txid        bigint      NOT NULL,                 -- pg_current_xact_id()::text::bigint  (xid8, NOT AWS's
                                                      --   32-bit varchar(16) — that wraps around and is ambiguous)
  c_lsn         pg_lsn,                               -- pg_current_wal_lsn() at capture — ordering vs data
  c_event       text        NOT NULL,                 -- 'ddl_command_end' | 'sql_drop'
  c_tag         text        NOT NULL,                 -- 'CREATE TABLE' | 'ALTER TABLE' | 'DROP TABLE' | 'COMMENT' | …
  c_obj_schema  text,                                 -- schema_name
  c_obj_identity text,                                -- object_identity
  c_rel_oid     oid,                                  -- affected pg_class OID (pg_event_trigger_ddl_commands.objid)
  c_columns     jsonb,                                -- STRUCTURED resulting column set (see below) — the payload
  c_dropped     jsonb,                                -- for sql_drop: dropped objects from pg_event_trigger_dropped_objects()
  c_ddl_text    text                                  -- raw current_query(), kept for LINEAGE/DEBUG ONLY, never replayed
);

-- SECURITY DEFINER inserts by a non-owner role also need the implicit sequence:
GRANT USAGE, SELECT ON SEQUENCE walrus.ddl_audit_c_key_seq TO walrus_writer;
```

`c_columns` is the load-bearing payload — a snapshot read from `pg_attribute` *after* the command:

```jsonc
[ { "name": "email", "format_type": "varchar(320)", "attnum": 4,
    "not_null": true, "default": null, "is_generated": false, "comment": null }, … ]
```

> **This replaces AWS's vestigial `c_oid`/`c_name`** (documented "for future use", always written
> `0`/`''`) and its `c_ddlqry`-as-payload. Keeping raw text as the payload would contradict
> `architecture.md`'s own schema-diff mandate; here raw text is lineage-only.

### 3.4 The two event triggers

Both are needed — verified [24][25][26]:

```sql
-- (1) creates / alters / comments — snapshots the resulting column set
CREATE FUNCTION walrus.intercept_ddl() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, walrus AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    IF r.command_tag IN ('CREATE TABLE','CREATE TABLE AS','ALTER TABLE','COMMENT') THEN
      INSERT INTO walrus.ddl_audit
        (c_txid, c_lsn, c_event, c_tag, c_obj_schema, c_obj_identity, c_rel_oid, c_columns, c_ddl_text)
      VALUES (pg_current_xact_id()::text::bigint, pg_current_wal_lsn(), 'ddl_command_end',
              r.command_tag, r.schema_name, r.object_identity, r.objid,
              walrus.snapshot_columns(r.objid),   -- reads pg_attribute → jsonb array (above)
              current_query());
    END IF;
  END LOOP;
END $$;
CREATE EVENT TRIGGER walrus_intercept_ddl ON ddl_command_end
  EXECUTE FUNCTION walrus.intercept_ddl();

-- (2) drops — the ONLY reliable place to enumerate dropped COLUMN identity + CASCADE victims
CREATE FUNCTION walrus.intercept_drop() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, walrus AS $$
DECLARE d record;
BEGIN
  FOR d IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
    IF d.object_type IN ('table','table column') THEN     -- 'table column' + objsubid=attnum = a dropped column
      INSERT INTO walrus.ddl_audit (c_txid, c_lsn, c_event, c_tag, c_obj_schema, c_obj_identity, c_dropped, c_ddl_text)
      VALUES (pg_current_xact_id()::text::bigint, pg_current_wal_lsn(), 'sql_drop', TG_TAG,
              d.schema_name, d.object_identity,
              jsonb_build_object('object_type', d.object_type, 'schema', d.schema_name,
                                 'identity', d.object_identity, 'objsubid', d.objsubid),
              current_query());
    END IF;
  END LOOP;
END $$;
CREATE EVENT TRIGGER walrus_intercept_drop ON sql_drop
  EXECUTE FUNCTION walrus.intercept_drop();
```

Notes, all verified:

- `ddl_command_end` fires **after execution but pre-commit**, so the catalog reads as already-changed
  **and** the audit `INSERT` enters the WAL stream in this transaction [24][25].
- `sql_drop` fires **just before** `ddl_command_end` for anything that drops objects, and
  `pg_event_trigger_dropped_objects()` reports a dropped column as `object_type = 'table column'` with
  `objsubid = attnum` [25].
- **Privilege attribution (a correction to `architecture.md`):** **superuser is required to `CREATE
  EVENT TRIGGER`** — *not* to create the function. `SECURITY DEFINER` is an **optional design choice** so
  the function can write the protected audit table regardless of the invoking user; it is not a hard
  requirement [26][27].

### 3.5 How the sink consumes it

```sql
ALTER PUBLICATION walrus_pub ADD TABLE walrus.ddl_audit;   -- unless FOR ALL TABLES
```

Because `walrus.ddl_audit` is **published**, its `INSERT`s flow through the same slot as DML, in commit
order. When the sink decodes a `ddl_audit` insert it:

1. writes a `ddl_manifest` row (the DDL event, stamped with `c_lsn`);
2. bumps the affected table's **structural `schema_version`**; and
3. **cuts a fresh Parquet file** — the *homogeneous-file rule*: every Parquet file carries exactly one
   `schema_version`, so the loader applies the structural change at the correct LSN, *before* merging any
   later-schema data ([architecture.md](./architecture.md#per-change-type-handling-schema-evolution-semantics)).

`ddl_manifest` and `schema_registry` are low-volume and **never pruned** — they are the history needed to
reconstruct any table at any `schema_version`.

### 3.6 Limitations & backstops

Event triggers are **not exhaustive** — these gaps are real and the design accounts for them [24][25]:

- **Globals fire nothing.** No event trigger fires for shared/global objects (roles, databases,
  tablespaces, `ALTER SYSTEM`) or on event triggers themselves — **acceptable**, walrus replicates
  *tables*, not globals.
- **`TRUNCATE` fires no event trigger** — it is delivered *natively* by pgoutput as a `Truncate` message
  (`Byte1('T')`, option bits `1`=CASCADE, `2`=RESTART IDENTITY) and handled on the message path, not the
  audit table [proto-version.md §4](./proto-version.md#4-the-message-catalog-decoded-byte-by-byte).
- **DDL nested in a function/procedure body is not captured** (the outer tag is `CREATE FUNCTION`) —
  matches DMS's documented limitation [6].
- **CASCADE victims** (`DROP SCHEMA/TYPE … CASCADE`) carry a non-table `TG_TAG`; only the `sql_drop`
  trigger catches their table/column victims [25].
- **Framing fix (a correction to `architecture.md`):** it is **overstated** to say `ddl_command_end`
  "does not enumerate `DROP COLUMN`." An `ALTER TABLE … DROP COLUMN` **is** visible in `ddl_command_end`
  as an `ALTER TABLE` row; it is only the *dropped-column identity* (`object_type 'table column'`,
  `objsubid = attnum`) and CASCADE victims that require `sql_drop`. The `sql_drop` trigger is still the
  correct move — just don't rely on the absolute "empty for drops" claim [24][25].
- **DMS apply-engine skip-list → our requirements checklist.** DMS explicitly does *not* apply several
  `ALTER` sub-ops (column `SET/DROP DEFAULT`, `SET/DROP NOT NULL`, PK redefinition, partition
  `ADD/DROP/TRUNCATE`) [6]; treat that list as walrus's explicit per-item to-handle checklist rather than
  a silent gap.

**Backstop (defense in depth):** reconcile captured schema against pgoutput **`Relation`** messages
(which carry live column metadata) — if a `Relation` shows a shape the registry doesn't know, flag drift
— plus a periodic schema-diff audit job.

### 3.7 Preflight

Source Postgres is **self-managed with full superuser**, so the triggers install directly. Still,
bootstrap **verifies** `walrus.ddl_audit` + both event triggers exist and that `CREATE EVENT TRIGGER` is
permitted; **missing or impermissible → terminal** (without the triggers the sink silently misses every
schema change). *(If a future target is managed Postgres — RDS `rds_superuser`, Cloud SQL — this same
preflight is where you'd detect the restriction and fail fast rather than drift.)*

---

## 4. Kubernetes pod lifecycle

> **The requirement:** the service runs on Kubernetes and **keeps working through pod churn** — a
> rescheduled, evicted, or drained pod recovers on its own and resumes exactly where it left off, with no
> data loss and no babysitting ([architecture.md north star](./architecture.md)). That falls out of
> durable checkpoints + fail-fast bootstrap **plus** a clean shutdown path — the last of which is missing
> from the master sketch and specified here.

### 4.1 Topology & the real correctness backstop

**Decision (locked): `StatefulSet` with `replicas=1`** [30]. Stable identity, and Kubernetes will **not**
create the replacement pod until the prior one is fully terminated — strong split-brain prevention with
**no election code**. The cost is a brief terminate-then-recreate gap during a rolling update, which is
fine for an eventually-consistent CDC consumer.

But the **real** correctness guarantee is not Kubernetes — it is **Postgres itself**: a logical
replication slot permits **exactly one active consuming connection**; a second concurrent
`START_REPLICATION` on an active slot **fails** [4][29]. So even if Kubernetes ever briefly ran two pods
(a Deployment surge, a pod lingering on an unreachable node), only one can stream. A replacement pod must
therefore treat the transient *"replication slot … is active"* error as **retry-with-backoff**, not
fatal, while the old holder releases.

*Alternative (not chosen):* `Deployment replicas=1` + `coordination.k8s.io` **Lease** leader election
[28] buys near-zero-downtime handoff, but it is more moving parts and the Lease is only an **anti-thrash**
mechanism — the slot rule is still the backstop. Adopt it only if minimizing the failover gap becomes a
hard requirement.

### 4.2 Startup

Run an ordered, fail-fast bootstrap **before** any WAL is read (the full preflight checklist lives in
[architecture.md](./architecture.md#startup--bootstrap-fail-fast-preflight)):

- **`initContainers`** (run to completion, in order) idempotently ensure prerequisites: the single slot +
  publication exist, control-plane migrations are current, and source/control Postgres + S3 are reachable
  [22].
- A **generous `startupProbe`** gates the possibly-slow bootstrap and initial WAL catch-up / backfill.
  **Verified semantics:** while a `startupProbe` is configured and not yet succeeded, Kubernetes runs
  **neither liveness nor readiness** probes [22] — so a legitimately long initial load can never be
  killed mid-progress. Size it `failureThreshold × periodSeconds` well above worst-case catch-up.
- **Fail-fast taxonomy.** *Terminal* misconfig (wrong `wal_level`, missing publication, slot
  `wal_status = 'lost'`, missing DDL triggers, incompatible `proto_version`, corrupt local checkpoint) →
  exit non-zero → `CrashLoopBackOff` — loud and immediate. *Transient* errors (slot momentarily active for
  another PID, S3 5xx, network blips) → **retry in-process with backoff**, so the replication connection
  isn't churned.

### 4.3 Probes — get these exactly right

This is where the master sketch has a **self-healing hazard**, corrected here:

| Probe | Purpose | walrus wiring |
|---|---|---|
| `startupProbe` | gate slow bootstrap; suppress the other two until done | generous `failureThreshold × periodSeconds` |
| `readinessProbe` | keep the pod out of rotation / PDB accounting until working | connected + slot/lease held + not-terminating |
| `livenessProbe` | **true deadlock detection only** | replication loop thread is alive / making progress |

> **⚠ Correction to `architecture.md`:** do **not** set `livenessProbe = "slot lag < cap"`. A pod
> legitimately **catching up after an outage** has *high* lag *by definition* — a lag-based liveness
> probe would kill it exactly when it's doing its job, producing a restart loop that never catches up.
> Gate slow catch-up behind the `startupProbe`; reserve liveness for genuine hangs.

### 4.4 Steady state

- **Slot liveness** (keep, from [architecture.md §1.9](./architecture.md#19-slot-liveness--heartbeat--keepalive)):
  a **heartbeat** (a published `walrus.heartbeat` write, or `pg_logical_emit_message`, PG14+) so an idle
  publication can't pin WAL; and **unconditional keepalive** standby feedback on a sub-`wal_sender_timeout`
  (60s) interval — **separate** from the durability checkpoint — so the walsender never drops the
  connection [3][31].
- **PodDisruptionBudget** (**missing from `architecture.md`, add it):** use **`maxUnavailable: 1`** or
  **no PDB at all**. A single-replica PDB with **`minAvailable: 1`** (= the replica count) makes the pod
  **unevictable** and permanently **blocks `kubectl drain` / node upgrades** [32] — directly contradicting
  the self-healing-through-node-drain north star.

### 4.5 Graceful shutdown — the missing piece

`architecture.md` covers startup, failover-resume, and decommission slot-drop, but **not normal-operation
shutdown**. Specified here.

**The exact Kubernetes termination sequence** (verified [22][33] — and correcting the common
"all in parallel" belief):

1. The pod is marked **`Terminating`** and the **`terminationGracePeriodSeconds` countdown starts
   immediately**.
2. **Concurrently**, the control plane removes the pod from Service `EndpointSlice`s (irrelevant to a
   non-serving consumer, but it's why the countdown is charged from T=0).
3. **Sequentially on the node:** the kubelet runs the **`preStop` hook first**, and **only after it
   completes** sends **`SIGTERM` to PID 1** — *not* in parallel. Both the preStop hook and the SIGTERM
   handler are charged against the **same single grace budget**.
4. If anything is still running when the grace period expires, the kubelet sends **`SIGKILL`**.

**Design consequences:**

- **Make the Rust process `SIGTERM`-aware directly** and ensure the signal reaches it — it must be **PID
  1**, or run under `tini`, or use the exec form of the entrypoint. A non-exec shell entrypoint
  **swallows `SIGTERM`** and the pod is `SIGKILL`ed with an in-flight batch lost.
- **Prefer direct `SIGTERM` handling and *skip* the `preStop` hook** for this non-serving consumer — any
  preStop time is *subtracted* from the same grace budget the drain needs, and with no preStop, `SIGTERM`
  arrives at T=0 with the full budget available [33].

**On `SIGTERM`, drain in order:**

1. **stop requesting new WAL** (stop consuming from the stream);
2. **finish and flush the in-flight** Arrow → Parquet → S3 batch and **COMMIT its manifest row**;
3. send a **final standby status update** advancing `confirmed_flush_lsn` to the last durable batch —
   **never past an open streamed txn** ([architecture.md §1.6](./architecture.md#16-large-transaction-safety));
4. send **`CopyDone`** and close the replication connection cleanly;
5. **DO NOT drop the slot.**

> **Why step 5 is the whole point.** A logical replication slot **persists independently of the
> connection**, is crash-safe, and retains WAL; a replacement pod's `START_REPLICATION` resumes at the
> greater of the requested LSN and `confirmed_flush_lsn` [4][29][34][35]. So **shutdown is just a
> resume** — even an ungraceful `SIGKILL` only means the slot sits at its last checkpoint and a few recent
> changes re-stream, which the loader's raw `APPEND` + `MERGE` **de-duplicates** (at-least-once →
> effectively-once). Graceful drain simply minimizes that replay.

**Tuning:**

- Set **`terminationGracePeriodSeconds` to the measured worst-case drain** (batch encode + Parquet write +
  S3 PUT + manifest commit + final feedback + close under peak WAL) — typically **60–120s**, **not the 30s
  default**.
- **`wal_sender_timeout` during the drain** (add this — `architecture.md` only ties it to steady-state
  keepalive): a long drain that stops replying to keepalives may be **severed at `wal_sender_timeout`
  (60s)** [3][31]. This is **harmless to correctness** (the slot persists), but to make sure the *final*
  `confirmed_flush_lsn` feedback lands, either keep replying to keepalives during the drain or keep the
  drain shorter than 60s.

### 4.6 The loader's shutdown differs

On `SIGTERM` the loader finishes the current **Phase-A append** + **Phase-B transform**, commits **both
watermarks** (`raw_appended_lsn`, `transformed_lsn`), releases its **table-ownership lease**, and releases
the **DuckDB single-writer lock** / closes the file cleanly — so a replacement loader can immediately take
ownership without a stale lock or a half-applied batch
([architecture.md §2](./architecture.md#component-2--data-sink-walrus-loader)).

### 4.7 Decommission — the only place the slot is dropped

The **sole** place `DROP_REPLICATION_SLOT` is ever issued is an explicit **decommission job** — never a
normal pod exit. An abandoned slot pins WAL forever [34], so decommission must drop it; every other
restart leaves it in place to resume. (Total-restart on slot *loss* is the separate disaster path in
[architecture.md §1.8](./architecture.md#18-single-slot-for-life--total-restart).)

---

## 5. What this doc supersedes in `architecture.md`

This doc is authoritative for the three areas; the following `architecture.md` rows are **corrected** here
(apply as edits, or mark each "superseded by `walrus-pg-sink.md`"):

| `architecture.md` location | Issue | Corrected in |
|---|---|---|
| Type table — `interval` → `Interval(MonthDayNano)` | **Wrong**: arrow-rs errors writing it to Parquet; single-int64-µs is lossy | [§2.4 interval](#interval--three-signed-integer-columns) |
| Type table — ranges/complex → `Utf8` fallback | **Underspecified**: ranges need the 5-col decomposition; multiranges unmentioned; `bit` needs `'0'/'1'`; geometric `path` needs `is_closed`; PostGIS absent | [§2.3](#23-the-full-type-table)–[§2.5](#25-tier-3-canonical-text-carriers) |
| Type table — `numeric` → `Decimal128/Decimal256` | **Over-promises**: `Decimal256` doesn't round-trip (DuckDB downcasts p>38 → DOUBLE) | [§2.5](#unconstrained--38-digit-numeric) |
| DDL audit block — `c_oid`/`c_name`/`c_ddlqry`-as-payload, `c_txn varchar(16)` | **Inconsistent/vestigial**: use structured `c_columns`/`c_dropped` jsonb; xid8/bigint txid; GRANT the sequence | [§3.3](#33-the-audit-table) |
| DDL — "`ddl_command_end` does not enumerate `DROP COLUMN`" | **Overstated framing** | [§3.6](#36-limitations--backstops) |
| DDL — SECURITY DEFINER implied as the privileged part | **Misattributed**: superuser is for `CREATE EVENT TRIGGER`, not the function | [§3.4](#34-the-two-event-triggers) |
| Lifecycle — no graceful-shutdown path | **Missing**: SIGTERM drain, final checkpoint, don't-drop-slot, grace period, preStop, PID-1 | [§4.5](#45-graceful-shutdown--the-missing-piece) |
| K8s — `livenessProbe = "slot lag < cap"` | **Self-healing hazard**: kills a pod that's catching up | [§4.3](#43-probes--get-these-exactly-right) |
| K8s — no PDB guidance | **Dangerous default**: single-replica `minAvailable: 1` blocks node drain | [§4.4](#44-steady-state) |
| K8s — StatefulSet vs Lease without the backstop | **Under-explained**: the Postgres single-active-slot rule is the real guarantee | [§4.1](#41-topology--the-real-correctness-backstop) |
| K8s — `wal_sender_timeout` only tied to steady-state | **Gap**: also governs the shutdown drain | [§4.5](#45-graceful-shutdown--the-missing-piece) |
| Type/geo — pinned DuckDB 1.4.x | **Forward-compat note**: DuckDB ≥1.5 adds core `GEOMETRY` + `VARIANT` → revisit PostGIS/json mappings on the next-LTS bump | [§2.4 PostGIS](#postgis) |

---

## Sources

Primary sources are PostgreSQL / AWS / DuckDB / Apache / Kubernetes docs; blogs corroborate. Bracketed
numbers reuse `architecture.md`'s where they overlap.

- [3] PostgreSQL — Logical Streaming Replication Protocol — https://www.postgresql.org/docs/current/protocol-replication.html
- [4] PostgreSQL — Replication slots (single active consumer; resume from `confirmed_flush_lsn`) — https://www.postgresql.org/docs/current/view-pg-replication-slots.html
- [5] PostgreSQL — Logical Decoding Concepts (no DDL) — https://www.postgresql.org/docs/current/logicaldecoding-explanation.html
- [6] AWS DMS — Using a PostgreSQL database as a source (`awsdms_ddl_audit`, data-type mapping, DDL limitations) — https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.PostgreSQL.html
- [8] arrow-rs — `parquet::arrow::ArrowWriter` — https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowWriter.html
- [9] DuckDB — `parquet_reader.cpp` (DECIMAL precision>38 → DOUBLE downcast) — https://raw.githubusercontent.com/duckdb/duckdb/main/extension/parquet/parquet_reader.cpp
- [10] Apache Parquet — Logical Types (DECIMAL, INTERVAL, UUID, TIMESTAMP) — https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
- [11] PostgreSQL — Date/Time Types (`interval` internals: months/days/µs) — https://www.postgresql.org/docs/current/datatype-datetime.html
- [12] arrow-rs — `arrow::datatypes::DataType` — https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
- [13] DuckDB — Reading Parquet (type inference; ENUM/UUID) — https://duckdb.org/docs/current/data/parquet/overview
- [14] Apache Arrow — Columnar Format (temporal / interval types) — https://arrow.apache.org/docs/format/Columnar.html
- [15] DuckDB — INTERVAL type + functions; nested types — https://duckdb.org/docs/current/sql/data_types/interval / https://duckdb.org/docs/stable/sql/functions/nested
- [16] Debezium — PostgreSQL connector type mapping — https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- [17] DuckDB — Data Types overview (no RANGE type; LIST/STRUCT/MAP) — https://duckdb.org/docs/current/sql/data_types/overview
- [18] DuckDB — Parquet import guide — https://duckdb.org/docs/current/guides/file_formats/parquet_import
- [19] DuckDB — TIMESTAMP / TIMESTAMPTZ — https://duckdb.org/docs/current/sql/data_types/timestamp
- [20] arrow-rs — MonthDayNano interval → Parquet (unsupported/error) — https://github.com/apache/arrow-rs/issues/1666
- [21] PostgreSQL — Range Types + range functions (`lower`/`upper`/`*_inc`/`isempty`) — https://www.postgresql.org/docs/current/rangetypes.html / https://www.postgresql.org/docs/17/functions-range.html
- [22] Kubernetes — Pod Lifecycle; container probes (startup suppresses liveness/readiness) — https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/ / https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/
- [23] PostgreSQL — Geometric Types — https://www.postgresql.org/docs/17/datatype-geometric.html
- [24] PostgreSQL — Event Triggers: overview & firing matrix — https://www.postgresql.org/docs/current/event-trigger-definition.html / https://www.postgresql.org/docs/16/event-trigger-matrix.html
- [25] PostgreSQL — Event-trigger functions (`pg_event_trigger_ddl_commands` / `pg_event_trigger_dropped_objects`) — https://www.postgresql.org/docs/current/functions-event-triggers.html
- [26] PostgreSQL — `CREATE EVENT TRIGGER` (superuser required) — https://www.postgresql.org/docs/current/sql-createeventtrigger.html
- [27] PostgreSQL — `CREATE FUNCTION` / `SECURITY DEFINER` — https://www.postgresql.org/docs/current/sql-createfunction.html
- [28] Kubernetes — Leases (leader election) — https://kubernetes.io/docs/concepts/architecture/leases/
- [29] G. Morling — confirmed_flush_lsn vs restart_lsn — https://www.morling.dev/blog/postgres-replication-slots-confirmed-flush-lsn-vs-restart-lsn/
- [30] Kubernetes — StatefulSets — https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/
- [31] PostgreSQL — Replication config (`wal_sender_timeout`) — https://www.postgresql.org/docs/current/runtime-config-replication.html
- [32] Kubernetes — Configure a PodDisruptionBudget — https://kubernetes.io/docs/tasks/run-application/configure-pdb/
- [33] Kubernetes — Container Lifecycle Hooks (preStop precedes SIGTERM) — https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/
- [34] G. Morling — The Insatiable Postgres Replication Slot — https://www.morling.dev/blog/insatiable-postgres-replication-slot/
- [35] PostgreSQL — `START_REPLICATION` resume semantics — https://www.postgresql.org/docs/current/protocol-replication.html

> **Research caveats to remember (verified):** DuckDB has no RANGE/MULTIRANGE type in any version, but
> gained a **core `GEOMETRY`** and a `VARIANT` composite type in **v1.5** (not the pinned 1.4.x LTS) — so
> the PostGIS and json/hstore mappings are worth revisiting on the next-LTS bump. The claim that
> `pg_event_trigger_ddl_commands()` "returns empty for DROP" is **overstated** — the reliable rule is
> "use `sql_drop` + `pg_event_trigger_dropped_objects()` for dropped tables/columns"; `DROP COLUMN`'s
> `ALTER` is itself visible in `ddl_command_end`.
