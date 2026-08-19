//! `string_enum!`: one variant table -> an enum plus its two-way `&'static str` mapping.
//!
//! walrus stores several closed sets as plain Postgres `text` (`file_manifest.kind`/`.status`,
//! `table_reload.flavor`/`.status`). Each needs the same pair of impls, and hand-writing them means
//! every legal string is typed twice — once in `as_str`, once in `FromStr` — so the two directions
//! can drift. This macro makes the variant table the single source of both.

/// Declare a `text`-column enum and its exact persisted strings in one table.
///
/// ```ignore
/// string_enum! {
///     /// Doc comments are captured and re-emitted onto the generated enum.
///     #[derive(Debug, Clone, Copy, PartialEq, Eq)]
///     pub enum ManifestKind {
///         error = ParseEnumError;
///         column = "file_manifest.kind";
///         Snapshot => "snapshot",
///         Stream   => "stream",
///     }
/// }
/// ```
#[macro_export]
macro_rules! string_enum {
    (
        // `:meta` captures complete attribute bodies, including desugared `///` docs and caller
        // derives. Re-emitting them as caller tokens lets `sqlx::Type` resolve in `control`.
        $(#[$attr:meta])*
        // `:vis` alone may match nothing; one arm covers public, restricted, and private enums.
        $vis:vis enum $name:ident {
            // `:path` preserves the caller's typed error. The column and persisted values use
            // `:literal`, not `:expr`, so expressions and macros are rejected at the call site.
            error = $error:path;
            column = $column:literal;
            // `:ident` precisely names enum variants; `$(,)?` accepts one trailing comma.
            $($variant:ident => $text:literal),* $(,)?
        }
    ) => {
        $(#[$attr])*
        $vis enum $name {
            $($variant,)*
        }

        impl $name {
            /// The exact string persisted in the control DB.
            #[must_use]
            $vis const fn as_str(self) -> &'static str {
                match self {
                    $($name::$variant => $text,)*
                }
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = $error;

            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                match s {
                    $($text => Ok($name::$variant),)*
                    other => Err(<$error>::new($column, other)),
                }
            }
        }
    };
}

#[cfg(test)]
#[path = "string_enum_test.rs"]
mod tests;
