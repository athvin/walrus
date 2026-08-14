//! Typed domain IDs — newtypes over the bare `i64` primary keys the control plane hands around.
//!
//! `ManifestId` extends the [`Lsn`](crate::Lsn) newtype pattern to a `file_manifest` row's id, so it
//! can't be silently swapped for another bare `i64` (a manifest id vs an epoch vs a schema version).
//! `ManifestId`, [`EpochNo`], [`SchemaVersionNo`], and [`ReloadId`] share the same transparent
//! `int8` boundary while remaining distinct types inside Rust.

/// A `file_manifest` row's primary key (`id`): returned by `insert_ready`, claimed as
/// `ManifestRow::id`, and retired through the loader's Phase-A lifecycle (`delete_claimed` /
/// `mark_failed`). Those APIs live in downstream crates, so `common` cannot link to them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ManifestId(pub i64);

impl std::fmt::Display for ManifestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for ManifestId {
    fn from(v: i64) -> Self {
        ManifestId(v)
    }
}

impl From<ManifestId> for i64 {
    fn from(id: ManifestId) -> Self {
        id.0
    }
}

/// The control plane's **generation counter** (`replication_state.epoch`): bumped when the lifelong
/// replication slot is lost and a total restart opens a new generation (§1.8). It namespaces every
/// control-plane row and every S3 key prefix — it is *not* a row id, and it must never be confused
/// with a [`ManifestId`], a schema version, or a table OID.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct EpochNo(pub i64);

impl std::fmt::Display for EpochNo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for EpochNo {
    fn from(v: i64) -> Self {
        EpochNo(v)
    }
}

impl From<EpochNo> for i64 {
    fn from(value: EpochNo) -> Self {
        value.0
    }
}

/// A relation's structural schema-version number.
///
/// The transparent serde representation preserves the bare JSON number stored in Parquet metadata.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct SchemaVersionNo(pub i64);

impl std::fmt::Display for SchemaVersionNo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for SchemaVersionNo {
    fn from(value: i64) -> Self {
        SchemaVersionNo(value)
    }
}

impl From<SchemaVersionNo> for i64 {
    fn from(value: SchemaVersionNo) -> Self {
        value.0
    }
}

/// A `table_reload` attempt's monotonic `bigserial` primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReloadId(pub i64);

impl std::fmt::Display for ReloadId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for ReloadId {
    fn from(value: i64) -> Self {
        ReloadId(value)
    }
}

impl From<ReloadId> for i64 {
    fn from(value: ReloadId) -> Self {
        value.0
    }
}

/// Postgres `int8` support (feature `sqlx`): `ManifestId` binds and decodes exactly as its inner
/// `i64` — the transparent-newtype trick — so a `bigint` column round-trips with no SQL cast. Mirrors
/// [`Lsn`](crate::Lsn)'s `sqlx_support`; hand-written rather than derived so `common`'s `sqlx` dep
/// needn't pull the `macros` feature. Array binds (`&[ManifestId]`) convert to `&[i64]` at the call
/// site — a manual `Type` impl carries no `PgHasArrayType`.
#[cfg(feature = "sqlx")]
mod sqlx_support {
    use super::{EpochNo, ManifestId, ReloadId, SchemaVersionNo};
    use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef};
    use sqlx::{Decode, Encode, Postgres, Type};

    impl Type<Postgres> for ManifestId {
        fn type_info() -> PgTypeInfo {
            <i64 as Type<Postgres>>::type_info()
        }
        fn compatible(ty: &PgTypeInfo) -> bool {
            <i64 as Type<Postgres>>::compatible(ty)
        }
    }

    impl Encode<'_, Postgres> for ManifestId {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            <i64 as Encode<Postgres>>::encode_by_ref(&self.0, buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for ManifestId {
        fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
            Ok(ManifestId(<i64 as Decode<Postgres>>::decode(value)?))
        }
    }

    impl Type<Postgres> for EpochNo {
        fn type_info() -> PgTypeInfo {
            <i64 as Type<Postgres>>::type_info()
        }
        fn compatible(ty: &PgTypeInfo) -> bool {
            <i64 as Type<Postgres>>::compatible(ty)
        }
    }

    impl Encode<'_, Postgres> for EpochNo {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            <i64 as Encode<Postgres>>::encode_by_ref(&self.0, buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for EpochNo {
        fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
            Ok(EpochNo(<i64 as Decode<Postgres>>::decode(value)?))
        }
    }

    impl Type<Postgres> for SchemaVersionNo {
        fn type_info() -> PgTypeInfo {
            <i64 as Type<Postgres>>::type_info()
        }
        fn compatible(ty: &PgTypeInfo) -> bool {
            <i64 as Type<Postgres>>::compatible(ty)
        }
    }

    impl Encode<'_, Postgres> for SchemaVersionNo {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            <i64 as Encode<Postgres>>::encode_by_ref(&self.0, buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for SchemaVersionNo {
        fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
            Ok(SchemaVersionNo(<i64 as Decode<Postgres>>::decode(value)?))
        }
    }

    impl Type<Postgres> for ReloadId {
        fn type_info() -> PgTypeInfo {
            <i64 as Type<Postgres>>::type_info()
        }
        fn compatible(ty: &PgTypeInfo) -> bool {
            <i64 as Type<Postgres>>::compatible(ty)
        }
    }

    impl Encode<'_, Postgres> for ReloadId {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            <i64 as Encode<Postgres>>::encode_by_ref(&self.0, buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for ReloadId {
        fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
            Ok(ReloadId(<i64 as Decode<Postgres>>::decode(value)?))
        }
    }
}

#[cfg(test)]
#[path = "ids_test.rs"]
mod tests;
