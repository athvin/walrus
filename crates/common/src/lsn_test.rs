use super::*;

#[test]
fn lsn_is_layout_identical_to_its_inner_u64() {
    assert_eq!(std::mem::size_of::<Lsn>(), std::mem::size_of::<u64>());
    assert_eq!(std::mem::align_of::<Lsn>(), std::mem::align_of::<u64>());
}

#[test]
fn parses_x_slash_y_form() {
    assert_eq!("0/199BAC8".parse::<Lsn>().unwrap().as_u64(), 0x199BAC8);
    assert_eq!("1/0".parse::<Lsn>().unwrap().as_u64(), 1u64 << 32);
    assert_eq!(
        "2/3B423D8".parse::<Lsn>().unwrap().as_u64(),
        (2u64 << 32) | 0x3B423D8
    );
    // low half is not zero-padded by Postgres and both halves are hex.
    assert_eq!(
        "A/B".parse::<Lsn>().unwrap().as_u64(),
        (0xA_u64 << 32) | 0xB
    );
}

#[test]
fn parses_bare_16_hex_form() {
    assert_eq!(
        "00000000019A2B3C".parse::<Lsn>().unwrap().as_u64(),
        0x19A2B3C
    );
    // leading zeros are optional.
    assert_eq!("19A2B3C".parse::<Lsn>().unwrap().as_u64(), 0x19A2B3C);
    assert_eq!("0".parse::<Lsn>().unwrap(), Lsn::ZERO);
    assert_eq!(
        "FFFFFFFFFFFFFFFF".parse::<Lsn>().unwrap().as_u64(),
        u64::MAX
    );
    // lowercase parses too (Display is the canonical uppercase form).
    assert_eq!("deadbeef".parse::<Lsn>().unwrap().as_u64(), 0xDEADBEEF);
}

#[test]
fn display_is_zero_padded_16_upper_hex() {
    assert_eq!(Lsn::new(0x19A2B3C).to_string(), "00000000019A2B3C");
    assert_eq!(Lsn::ZERO.to_string(), "0000000000000000");
    assert_eq!(Lsn::new(u64::MAX).to_string(), "FFFFFFFFFFFFFFFF");
    assert_eq!(Lsn::new(0xDEADBEEF).to_string().len(), 16);
    // matches the design's walrus_pg_sink_meta sample.
    assert_eq!(Lsn::new(0x1B4C000).to_string(), "0000000001B4C000");
}

#[test]
fn round_trips_through_display_and_from_str() {
    for raw in [0u64, 1, 0x199BAC8, 0x1B4C000, 0xFEDCBA9876543210, u64::MAX] {
        let lsn = Lsn::new(raw);
        assert_eq!(lsn.to_string().parse::<Lsn>().unwrap(), lsn);
    }
    // parse(X/Y) -> display -> parse is the identity on the value.
    let a = "0/199BAC8".parse::<Lsn>().unwrap();
    assert_eq!(a.to_string().parse::<Lsn>().unwrap(), a);
}

#[test]
fn u64_round_trips_through_from() {
    let lsn = Lsn::from(0x0000_0000_019A_2B3C_u64);
    assert_eq!(lsn, Lsn::new(0x0000_0000_019A_2B3C));
    assert_eq!(u64::from(lsn), 0x0000_0000_019A_2B3C);
    // `From` and the const constructors agree — neither is the "real" one.
    assert_eq!(u64::from(Lsn::ZERO), Lsn::ZERO.as_u64());
}

#[test]
fn bit_pattern_roundtrips_above_i64_max() {
    let lsn = Lsn::new(u64::MAX - 7);
    let encoded = lsn.to_sqlx_i64_bits();
    assert!(
        encoded < 0,
        "the high unsigned bit is preserved in the signed wire value"
    );
    assert_eq!(Lsn::from_sqlx_i64_bits(encoded), lsn);
}

#[test]
#[allow(clippy::op_ref, reason = "exercise the explicit borrowed Sub impl")]
fn subtraction_is_the_byte_distance() {
    assert_eq!(Lsn::new(500) - Lsn::new(200), 300);
    assert_eq!(Lsn::new(200) - Lsn::new(200), 0);
    assert_eq!(&Lsn::new(500) - &Lsn::new(200), 300);
}

#[test]
fn addition_advances_a_position() {
    assert_eq!(Lsn::new(100) + 24, Lsn::new(124));
    let mut lsn = Lsn::new(100);
    lsn += 24;
    assert_eq!(lsn, Lsn::new(124));
}

#[test]
fn saturating_sub_bytes_bottoms_out_at_zero() {
    assert_eq!(Lsn::new(300).saturating_sub_bytes(100), Lsn::new(200));
    assert_eq!(Lsn::new(100).saturating_sub_bytes(300), Lsn::ZERO);
}

#[test]
fn serde_round_trips_as_padded_string() {
    let lsn = Lsn::new(0x19A2B3C);
    let json = serde_json::to_string(&lsn).unwrap();
    assert_eq!(
        json, "\"00000000019A2B3C\"",
        "serialises as a padded STRING"
    );

    let back: Lsn = serde_json::from_str(&json).unwrap();
    assert_eq!(back, lsn);

    // deserialize also accepts the X/Y dialect, since it routes through FromStr.
    let from_xy: Lsn = serde_json::from_str("\"0/199BAC8\"").unwrap();
    assert_eq!(from_xy.as_u64(), 0x199BAC8);

    // a bare JSON number is NOT a valid Lsn (the padded-string contract).
    assert!(serde_json::from_str::<Lsn>("26879180").is_err());
}

/// The load-bearing invariant: sorting the *text* form equals sorting the *numeric* value.
#[test]
fn text_sort_equals_numeric_sort() {
    let vals = [
        Lsn::new(0xFF),
        Lsn::new(0x1),
        Lsn::ZERO,
        Lsn::new(0x100),
        Lsn::new(0x19A2B3C),
        Lsn::new(u64::MAX),
        Lsn::new(0x10),
        Lsn::new(0x1B4C000),
        Lsn::new(0xDEADBEEF),
    ];

    let mut by_numeric = vals;
    by_numeric.sort_by_key(|l| l.as_u64());

    let mut by_text = vals;
    by_text.sort_by_key(Lsn::to_string);

    let mut by_ord = vals;
    by_ord.sort();

    assert_eq!(by_text, by_numeric, "text sort must equal numeric sort");
    assert_eq!(by_ord, by_numeric, "derived Ord must be numeric");
    assert!(Lsn::ZERO < Lsn::new(1));
}

#[test]
fn rejects_garbage_and_overlong_input() {
    for bad in [
        "",                  // empty
        "xyz",               // non-hex
        "0/",                // empty low half
        "/0",                // empty high half
        "0/GG",              // non-hex low half
        "1FFFFFFFFFFFFFFFF", // 17 significant hex — overflows u64
        " 0/1",              // leading whitespace
        "+199",              // sign is not a hex digit
        "1FFFFFFFF/0",       // high half wider than u32
        "0/1FFFFFFFF",       // low half wider than u32
    ] {
        assert!(
            bad.parse::<Lsn>().is_err(),
            "expected {bad:?} to be rejected"
        );
    }
}
