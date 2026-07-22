use regex::Regex;

use crate::model::{Condition, FbUser};
use crate::prepared::PreparedCondition;

pub(super) fn condition_matches_prepared(
    condition: &Condition,
    prepared: Option<&PreparedCondition>,
    user: &FbUser,
) -> bool {
    let user_value = user.value_of(&condition.property);
    operator_matches_prepared(user_value, &condition.op, &condition.value, prepared)
}

fn operator_matches_prepared(
    user_value: &str,
    operator: &str,
    rule_value: &str,
    prepared: Option<&PreparedCondition>,
) -> bool {
    match operator {
        "LessThan" => {
            numeric_compare_prepared(user_value, rule_value, prepared, |left, right| left < right)
        }
        "LessEqualThan" => {
            numeric_compare_prepared(user_value, rule_value, prepared, |left, right| {
                left <= right
            })
        }
        "BiggerThan" => {
            numeric_compare_prepared(user_value, rule_value, prepared, |left, right| left > right)
        }
        "BiggerEqualThan" => {
            numeric_compare_prepared(user_value, rule_value, prepared, |left, right| {
                left >= right
            })
        }
        "Equal" => user_value == rule_value,
        "NotEqual" => user_value != rule_value,
        "Contains" => user_value.contains(rule_value),
        "NotContain" => !user_value.contains(rule_value),
        "StartsWith" => user_value.starts_with(rule_value),
        "EndsWith" => user_value.ends_with(rule_value),
        "MatchRegex" => regex_matches(user_value, rule_value, prepared, false),
        "NotMatchRegex" => regex_matches(user_value, rule_value, prepared, true),
        "IsOneOf" => string_set_matches(user_value, rule_value, prepared, false),
        "NotOneOf" => string_set_matches(user_value, rule_value, prepared, true),
        "IsTrue" => user_value.eq_ignore_ascii_case("true"),
        "IsFalse" => user_value.eq_ignore_ascii_case("false"),
        _ => false,
    }
}

fn numeric_compare_prepared(
    user_value: &str,
    rule_value: &str,
    prepared: Option<&PreparedCondition>,
    compare: impl FnOnce(f64, f64) -> bool,
) -> bool {
    match prepared {
        Some(PreparedCondition::Numeric(Some(rule_number))) => user_value
            .parse::<f64>()
            .ok()
            .filter(|number| number.is_finite())
            .is_some_and(|user_number| compare(user_number, *rule_number)),
        Some(PreparedCondition::Numeric(None)) => false,
        _ => numeric_compare(user_value, rule_value, compare),
    }
}

fn regex_matches(
    user_value: &str,
    rule_value: &str,
    prepared: Option<&PreparedCondition>,
    negate: bool,
) -> bool {
    let matched = match prepared {
        Some(PreparedCondition::Regex(Some(regex))) => regex.is_match(user_value),
        Some(PreparedCondition::Regex(None)) => return false,
        _ => {
            let Ok(regex) = Regex::new(rule_value) else {
                return false;
            };
            regex.is_match(user_value)
        }
    };
    matched != negate
}

fn string_set_matches(
    user_value: &str,
    rule_value: &str,
    prepared: Option<&PreparedCondition>,
    negate: bool,
) -> bool {
    let contains = match prepared {
        Some(PreparedCondition::StringSet(Some(values))) => values.contains(user_value),
        Some(PreparedCondition::StringSet(None)) => return false,
        _ => {
            let Some(values) = string_list(rule_value) else {
                return false;
            };
            values.iter().any(|value| value == user_value)
        }
    };
    contains != negate
}

fn numeric_compare(
    user_value: &str,
    rule_value: &str,
    compare: impl FnOnce(f64, f64) -> bool,
) -> bool {
    let (Ok(user_number), Ok(rule_number)) = (user_value.parse::<f64>(), rule_value.parse::<f64>())
    else {
        return false;
    };
    user_number.is_finite() && rule_number.is_finite() && compare(user_number, rule_number)
}

fn string_list(value: &str) -> Option<Vec<String>> {
    serde_json::from_str(value).ok()
}

#[cfg(test)]
mod tests {
    use super::operator_matches_prepared;

    fn operator_matches(user_value: &str, operator: &str, rule_value: &str) -> bool {
        operator_matches_prepared(user_value, operator, rule_value, None)
    }

    #[test]
    fn operators_match_all_dotnet_condition_matcher_fixtures() {
        // Copied from ConditionMatcherTests.cs at the pinned .NET protocol revision.
        let fixtures = [
            ("10", "BiggerThan", "9", true),
            ("10", "BiggerThan", "11", false),
            ("10", "BiggerEqualThan", "10", true),
            ("10", "BiggerEqualThan", "11", false),
            ("10", "LessThan", "11", true),
            ("10", "LessThan", "9", false),
            ("10", "LessEqualThan", "10", true),
            ("10", "LessEqualThan", "9", false),
            ("v1.0.0", "Equal", "v1.0.0", true),
            ("v1.1.0", "Equal", "v1.0.0", false),
            ("v1.1.0", "NotEqual", "v1.1.0", false),
            ("v1.1.0", "NotEqual", "v1.0.0", true),
            ("vvip", "Contains", "vip", true),
            ("vvip", "Contains", "sv", false),
            ("svip", "NotContain", "vv", true),
            ("svip", "NotContain", "vip", false),
            ("abc", "StartsWith", "ab", true),
            ("abc", "StartsWith", "b", false),
            ("abc", "EndsWith", "bc", true),
            ("abc", "EndsWith", "cd", false),
            ("color", "MatchRegex", "colou?r", true),
            ("colour", "MatchRegex", "colorr?", false),
            ("colouur", "NotMatchRegex", "colou?r", true),
            ("color", "NotMatchRegex", "colou?r", false),
            ("a", "IsOneOf", "[\"a\", \"b\"]", true),
            ("c", "IsOneOf", "[\"a\", \"b\"]", false),
            ("c", "NotOneOf", "[\"a\", \"b\"]", true),
            ("a", "NotOneOf", "[\"a\", \"b\"]", false),
            ("true", "IsTrue", "", true),
            ("TRue", "IsTrue", "", true),
            ("false", "IsFalse", "", true),
            ("falSE", "IsFalse", "", true),
            ("not-true-string", "IsTrue", "", false),
            ("not-false-string", "IsFalse", "", false),
        ];

        for (user, operator, rule, expected) in fixtures {
            assert_eq!(
                operator_matches(user, operator, rule),
                expected,
                "unexpected result for {user:?} {operator} {rule:?}"
            );
        }
    }

    #[test]
    fn malformed_and_unknown_operators_are_non_matches() {
        let fixtures = [
            ("a", "NotMatchRegex", "["),
            ("a", "NotOneOf", "not-json"),
            ("a", "IsOneOf", "null"),
            ("NaN", "BiggerThan", "0"),
            ("1", "BiggerThan", "NaN"),
            ("inf", "LessThan", "1"),
            ("a", "UnknownOperator", "a"),
        ];

        for (user, operator, rule) in fixtures {
            assert!(!operator_matches(user, operator, rule));
        }
    }
}
