// Every serde-carrying enum in `common` currently has a bare-scalar JSON form. `Op`, `Kind`, and
// `ReplicaIdentity` use serde's default external enum representation; `Tier` uses numeric
// `into`/`try_from` conversion. Each expected value comes from an exhaustive match with no `_` arm,
// making a new variant or payload a compile error before it can change the wire format.

use common::{Kind, Op, ReplicaIdentity, Tier};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

/// Serialize, compare against the golden scalar, prove it is a scalar, and round-trip.
fn assert_scalar_wire<T>(value: T, expected: &Value)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug + Copy,
{
    let serialized = serde_json::to_value(value);
    assert_eq!(
        serialized.as_ref().ok(),
        Some(expected),
        "wire form drifted for {value:?}; serialization error: {:?}",
        serialized.as_ref().err()
    );
    let Ok(wire) = serialized else {
        return;
    };
    assert!(
        wire.is_string() || wire.is_number(),
        "must stay a JSON scalar; an object or array means a variant gained a payload: {wire}"
    );
    let round_tripped = serde_json::from_value::<T>(wire);
    assert_eq!(
        round_tripped.as_ref().ok(),
        Some(&value),
        "round-trip failed for {value:?}: {:?}",
        round_tripped.as_ref().err()
    );
}

/// Exhaustive: no wildcard arm. Keys are documented in architecture.md section 1.4.
fn op_wire(op: Op) -> Value {
    match op {
        Op::Insert => json!("i"),
        Op::Update => json!("u"),
        Op::Delete => json!("d"),
        Op::Truncate => json!("t"),
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
        Kind::Snapshot => json!("snapshot"),
        Kind::Stream => json!("stream"),
        Kind::Reload => json!("reload"),
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
        ReplicaIdentity::Default => json!("default"),
        ReplicaIdentity::Nothing => json!("nothing"),
        ReplicaIdentity::Full => json!("full"),
        ReplicaIdentity::Index => json!("index"),
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
        Tier::One => json!(1),
        Tier::Two => json!(2),
        Tier::Three => json!(3),
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
            assert!(
                !source.contains(attribute),
                "{module} introduced enum tagging attribute {attribute}"
            );
        }
    }
}
