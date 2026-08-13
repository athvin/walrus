use super::*;

#[test]
fn renew_interval_is_one_third_of_a_normal_ttl() {
    assert_eq!(
        renew_interval(Duration::from_secs(30)),
        Duration::from_secs(10)
    );
}

#[test]
fn renew_interval_never_reaches_the_ttl() {
    for secs in [3, 5, 30, 300, 3600] {
        let ttl = Duration::from_secs(secs);
        assert!(
            renew_interval(ttl) < ttl,
            "renew must land strictly inside the {ttl:?} TTL"
        );
    }
}

#[test]
fn renew_interval_floors_at_one_second() {
    assert_eq!(renew_interval(Duration::from_secs(3)), MIN_RENEW_INTERVAL);
}
