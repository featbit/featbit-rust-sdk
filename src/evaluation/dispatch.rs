use md5::{Digest, Md5};

use crate::model::RolloutVariation;

pub(super) fn rollout_of_key(key: &str) -> f64 {
    let digest = Md5::digest(key.as_bytes());
    let Some(first_four) = digest
        .get(..4)
        .and_then(|bytes| <&[u8; 4]>::try_from(bytes).ok())
    else {
        return 0.0;
    };
    let signed = i32::from_le_bytes(*first_four);
    ((f64::from(signed)) / f64::from(i32::MIN)).abs()
}

pub(super) fn is_in_rollout(key: &str, rollout: &[f64]) -> bool {
    let [min, max] = rollout else {
        return false;
    };
    if !min.is_finite() || !max.is_finite() || min > max {
        return false;
    }
    if *min == 0.0 && 1.0 - max < 1e-5 {
        return true;
    }
    if *min == 0.0 && *max == 0.0 {
        return false;
    }
    let value = rollout_of_key(key);
    value >= *min && value <= *max
}

pub(super) fn is_percentage_split(variations: &[RolloutVariation]) -> bool {
    if variations.len() > 1 {
        return true;
    }
    variations.first().is_some_and(|variation| {
        !matches!(variation.rollout.as_slice(), [min, max] if *min == 0.0 && 1.0 - max < 1e-5)
    })
}

#[cfg(test)]
mod tests {
    use super::{is_in_rollout, rollout_of_key};

    // Compatibility fixtures copied verbatim from FeatBit .NET Server SDK commit
    // 974e2a7a557095b300e4e89da86df7d6fa894963,
    // tests/FeatBit.ServerSdk.Tests/Evaluation/DispatchAlgorithmTests.cs.
    const DOTNET_DISPATCH_FIXTURES: [(&str, f64); 3] = [
        ("test-value", 0.146_536_292_042_583_23),
        ("qKPKh1S3FolC", 0.910_591_969_266_533_9),
        (
            "3eacb184-2d79-49df-9ea7-edd4f10e4c6f",
            0.089_944_031_555_205_58,
        ),
    ];

    #[test]
    fn rollout_of_key_matches_dotnet_dispatch_algorithm_vectors() {
        for (key, expected) in DOTNET_DISPATCH_FIXTURES {
            assert_eq!(
                rollout_of_key(key).to_bits(),
                expected.to_bits(),
                "dispatch result changed for {key}"
            );
        }
    }

    #[test]
    fn rollout_ranges_handle_protocol_boundaries_and_invalid_data() {
        for key in DOTNET_DISPATCH_FIXTURES.map(|(key, _)| key) {
            assert!(is_in_rollout(key, &[0.0, 1.0]));
            assert!(!is_in_rollout(key, &[0.0, 0.0]));
        }

        assert!(!is_in_rollout("key", &[]));
        assert!(!is_in_rollout("key", &[0.0]));
        assert!(!is_in_rollout("key", &[0.8, 0.2]));
        assert!(!is_in_rollout("key", &[f64::NAN, 1.0]));
        assert!(!is_in_rollout("key", &[0.0, f64::INFINITY]));
    }
}
