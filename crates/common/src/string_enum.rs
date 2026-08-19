//! `string_enum!`: one variant table -> a `Copy` enum plus its two-way `&'static str` mapping.
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
///     ManifestKind {
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
        // `///` reaches a matcher already desugared to `#[doc = "…"]`, so a `:literal` capture
        // is enough here; PR 24.3 widens this to `$(#[$attr:meta])*`.
        $(#[doc = $doc:literal])*
        $name:ident {
            // The caller owns the error taxonomy. `:path` accepts `ParseEnumError` or a qualified
            // equivalent; `:literal` pins the exact persisted DB column as structured context.
            error = $error:path;
            column = $column:literal;
            $($variant:ident => $text:literal),* $(,)?
        }
    ) => {
        $(#[doc = $doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $($variant,)*
        }

        impl $name {
            /// The exact string persisted in the control DB.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
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
