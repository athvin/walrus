//! `string_enum!`: one variant table -> a `Copy` enum plus its two-way `&'static str` mapping.
//!
//! walrus stores several closed sets as plain Postgres `text` (`file_manifest.kind`/`.status`,
//! `table_reload.flavor`/`.status`). Each needs the same pair of impls, and hand-writing them means
//! every legal string is typed twice — once in `as_str`, once in `FromStr` — so the two directions
//! can drift. This macro makes the variant table the single source of both.

#[cfg(test)]
#[path = "string_enum_test.rs"]
mod tests;
