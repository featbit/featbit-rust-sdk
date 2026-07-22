use super::{FeatureFlag, Segment};

const DOTNET_FLAG_JSON: &str = include_str!("fixtures/dotnet-one-flag.json");
const DOTNET_SEGMENT_JSON: &str = include_str!("fixtures/dotnet-one-segment.json");

#[test]
fn feature_flag_deserializes_from_pinned_dotnet_wire_fixture() {
    let flag: FeatureFlag =
        serde_json::from_str(DOTNET_FLAG_JSON).expect(".NET flag fixture should deserialize");

    assert_eq!(flag.id, "174c7138-426d-4434-8a91-af8c00306de3");
    assert_eq!(flag.key, "example");
    assert_eq!(flag.updated_at, 1_674_871_495_616);
    assert_eq!(flag.variation_type, "boolean");
    assert_eq!(flag.variations.len(), 2);
    assert_eq!(flag.variations[0].value, "true");
    assert_eq!(flag.target_users.len(), 2);
    assert_eq!(flag.target_users[0].key_ids.len(), 10);
    assert_eq!(flag.rules.len(), 2);
    assert_eq!(flag.rules[0].dispatch_key, None);
    assert_eq!(flag.rules[0].conditions.len(), 2);
    assert_eq!(flag.rules[1].dispatch_key.as_deref(), Some("keyId"));
    assert_eq!(flag.rules[1].variations[0].rollout, [0.0, 0.2]);
    assert_eq!(flag.disabled_variation_id, flag.variations[1].id);
    assert!(flag.is_enabled);
    assert!(flag.expt_include_all_targets);
    assert!(!flag.is_archived);
}

#[test]
fn segment_deserializes_from_pinned_dotnet_wire_fixture() {
    let segment: Segment =
        serde_json::from_str(DOTNET_SEGMENT_JSON).expect(".NET segment fixture should deserialize");

    assert_eq!(segment.id, "0779d76b-afc6-4886-ab65-af8c004273ad");
    assert_eq!(segment.updated_at, 1_674_885_283_583);
    assert_eq!(segment.included.len(), 10);
    assert_eq!(segment.excluded.len(), 5);
    assert_eq!(segment.rules.len(), 1);
    assert_eq!(segment.rules[0].conditions.len(), 4);
    assert_eq!(segment.rules[0].conditions[0].op, "LessEqualThan");
    assert_eq!(segment.rules[0].conditions[3].op, "IsTrue");
    assert!(!segment.is_archived);
}

#[test]
fn updated_at_accepts_protocol_integer_and_rejects_malformed_shapes() {
    let integer = r#"{"key":"flag","updatedAt":123}"#;
    let flag: FeatureFlag = serde_json::from_str(integer).expect("integer version should parse");
    assert_eq!(flag.updated_at, 123);

    for invalid in [
        r#"{"key":"flag","updatedAt":"not-a-date"}"#,
        r#"{"key":"flag","updatedAt":1.5}"#,
        r#"{"key":"flag","updatedAt":null}"#,
        r#"{"key":"flag","updatedAt":{}}"#,
    ] {
        assert!(serde_json::from_str::<FeatureFlag>(invalid).is_err());
    }
}
