//! Typed domain IDs — newtypes over the bare `i64` primary keys the control plane hands around.
//!
//! `ManifestId` extends the [`Lsn`](crate::Lsn) newtype pattern to a `file_manifest` row's id, so it
//! can't be silently swapped for another bare `i64` (a manifest id vs an epoch vs a schema version).
//! PR 8.4a lands `ManifestId` alone; `EpochNo`/`SchemaVersion`/`ReloadId` stay bare `i64` for now —
//! the same transparent-`int8` pattern below applies verbatim when they follow (deferred).

/// A `file_manifest` row's primary key (`id`): returned by [`insert_ready`](crate::insert_ready),
/// claimed as [`ManifestRow::id`](crate::ManifestRow), and retired through the loader's Phase-A
/// lifecycle ([`delete_claimed`](crate::delete_claimed) / [`mark_failed`](crate::mark_failed)).
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

/// Postgres `int8` support (feature `sqlx`): `ManifestId` binds and decodes exactly as its inner
/// `i64` — the transparent-newtype trick — so a `bigint` column round-trips with no SQL cast. Mirrors
/// [`Lsn`](crate::Lsn)'s `sqlx_support`; hand-written rather than derived so `common`'s `sqlx` dep
/// needn't pull the `macros` feature. Array binds (`&[ManifestId]`) convert to `&[i64]` at the call
/// site — a manual `Type` impl carries no `PgHasArrayType`.
#[cfg(feature = "sqlx")]
mod sqlx_support {
    use super::ManifestId;
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

    impl<'q> Encode<'q, Postgres> for ManifestId {
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
}

#[cfg(test)]
#[path = "ids_test.rs"]
mod tests;
