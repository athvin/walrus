// Every serde-carrying enum in `common` is unit-only, so its JSON form is a bare scalar under all
// four of serde's tagging strategies. Each expected value comes from an exhaustive match with no
// `_` arm, making a new variant or a payload a compile error before it can change the wire format.

use common::{Kind, Op, ReplicaIdentity, Tier};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

/// Serialize, compare against the golden scalar, prove it is a scalar, and round-trip.
fn assert_scalar_wire<T>(value: T, expected: &Value)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug + Copy,
{
    let wire = serde_json::to_value(value).expect("serializes");
    assert_eq!(&wire, expected, "wire form drifted for {value:?}");
    assert!(
        wire.is_string() || wire.is_number(),
        "must stay a JSON scalar; an object or array means a variant gained a payload: {wire}"
    );
    let round_tripped: T = serde_json::from_value(wire).expect("round-trips");
    assert_eq!(round_tripped, value);
}

/// Exhaustive: no wildcard arm. Keys are documented in architecture.md section 1.4.
fn op_wire(op: Op) -> Value {
    match op {
        Op::Insert => todo!("lock the insert wire value"),
        Op::Update => todo!("lock the update wire value"),
        Op::Delete => todo!("lock the delete wire value"),
        Op::Truncate => todo!("lock the truncate wire value"),
    }
}

#[test]
fn op_is_a_single_char_scalar() {
    for op in [Op::Insert, Op::Update, Op::Delete, Op::Truncate] {
        assert_scalar_wire(op, &op_wire(op));
    }
}

fn kind_wire(kind: Kind) -> Value {
    match kind {
        Kind::Snapshot => todo!("lock the snapshot wire value"),
        Kind::Stream => todo!("lock the stream wire value"),
        Kind::Reload => todo!("lock the reload wire value"),
    }
}

#[test]
fn kind_is_a_lowercase_scalar() {
    for kind in [Kind::Snapshot, Kind::Stream, Kind::Reload] {
        assert_scalar_wire(kind, &kind_wire(kind));
    }
}

fn replica_identity_wire(identity: ReplicaIdentity) -> Value {
    match identity {
        ReplicaIdentity::Default => todo!("lock the default wire value"),
        ReplicaIdentity::Nothing => todo!("lock the nothing wire value"),
        ReplicaIdentity::Full => todo!("lock the full wire value"),
        ReplicaIdentity::Index => todo!("lock the index wire value"),
    }
}

#[test]
fn replica_identity_is_a_lowercase_scalar() {
    for identity in [
        ReplicaIdentity::Default,
        ReplicaIdentity::Nothing,
        ReplicaIdentity::Full,
        ReplicaIdentity::Index,
    ] {
        assert_scalar_wire(identity, &replica_identity_wire(identity));
    }
}

fn tier_wire(tier: Tier) -> Value {
    match tier {
        Tier::One => todo!("lock tier one as a JSON number"),
        Tier::Two => todo!("lock tier two as a JSON number"),
        Tier::Three => todo!("lock tier three as a JSON number"),
    }
}

#[test]
fn tier_is_an_integer_scalar() {
    for tier in [Tier::One, Tier::Two, Tier::Three] {
        assert_scalar_wire(tier, &tier_wire(tier));
    }
}

/// The serde-bearing modules, scanned as source text.
const SERDE_MODULES: [(&str, &str); 3] = [
    ("sink_meta.rs", include_str!("../src/sink_meta.rs")),
    ("pg_shape.rs", include_str!("../src/pg_shape.rs")),
    (
        "type_descriptor.rs",
        include_str!("../src/type_descriptor.rs"),
    ),
];

#[test]
fn no_enum_tagging_attribute_is_introduced() {
    for (module, source) in SERDE_MODULES {
        for attribute in ["serde(tag", "serde(content", "serde(untagged"] {
            todo!("assert that {module} does not introduce {attribute}: {source}");
        }
    }
}
