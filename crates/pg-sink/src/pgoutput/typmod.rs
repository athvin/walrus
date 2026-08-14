//! `atttypmod` decoding. On the wire it's an `Int32`; `0xFFFFFFFF` is the `-1` "no modifier"
//! sentinel. For `numeric` it packs precision/scale as `((p << 16) | s) + 4`.

/// Decode a raw wire `atttypmod` into its signed value (`0xFFFFFFFF` → `-1`).
#[must_use]
pub fn atttypmod(raw: u32) -> i32 {
    raw.cast_signed()
}

/// Recover `numeric(p, s)` from a decoded `atttypmod`. `< 4` (including the `-1` sentinel) means
/// unconstrained → `None`; otherwise subtract 4 first, then unpack. e.g. `numeric(10, 2)` →
/// `atttypmod 655366` → `Some((10, 2))`.
#[must_use]
pub fn numeric_precision_scale(typmod: i32) -> Option<(u16, u16)> {
    if typmod < 4 {
        return None;
    }
    let packed = u32::try_from(typmod - 4).ok()?;
    let precision = u16::try_from((packed >> 16) & 0xFFFF).ok()?;
    let scale = u16::try_from(packed & 0xFFFF).ok()?;
    Some((precision, scale))
}

#[cfg(test)]
#[path = "typmod_test.rs"]
mod tests;
