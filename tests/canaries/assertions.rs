use std::collections::BTreeSet;

pub const DEFAULT_UNSUPPORTED_SQLSTATE: &str = "0A000";

#[must_use]
pub fn format_compatibility_mismatch(
    context: &str,
    expectation: &str,
    expected: &str,
    actual: &str,
) -> String {
    format!("{context}: expected {expectation} `{expected}`, got `{actual}`")
}

#[track_caller]
pub fn assert_sqlstate(actual: Option<&str>, expected: &str, context: &str) {
    let actual = actual.unwrap_or("<missing>");
    assert!(
        actual == expected,
        "{}",
        format_compatibility_mismatch(context, "SQLSTATE", expected, actual)
    );
}

#[track_caller]
pub fn assert_required_sqlstates<I, J>(actual: I, expected: J, context: &str)
where
    I: IntoIterator,
    I::Item: AsRef<str>,
    J: IntoIterator,
    J::Item: AsRef<str>,
{
    let actual = collect_sqlstates(actual);
    let expected = collect_sqlstates(expected);

    let missing = expected
        .difference(&actual)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let unexpected = actual
        .difference(&expected)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");

    assert!(
        missing.is_empty() && unexpected.is_empty(),
        "{}",
        format!(
            "{context}: SQLSTATE set mismatch; missing [{}]; unexpected [{}]; expected [{}]; actual [{}]",
            missing,
            unexpected,
            join_sqlstates(&expected),
            join_sqlstates(&actual)
        )
    );
}

#[track_caller]
pub fn assert_unsupported_refusal_sqlstate(actual: Option<&str>, context: &str) {
    assert_sqlstate(actual, DEFAULT_UNSUPPORTED_SQLSTATE, context);
}

fn collect_sqlstates<I>(codes: I) -> BTreeSet<String>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    codes
        .into_iter()
        .map(|code| code.as_ref().to_owned())
        .collect()
}

fn join_sqlstates(codes: &BTreeSet<String>) -> String {
    codes
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    use super::{
        DEFAULT_UNSUPPORTED_SQLSTATE, assert_required_sqlstates, assert_sqlstate,
        assert_unsupported_refusal_sqlstate, format_compatibility_mismatch,
    };

    #[test]
    fn format_mismatch_is_stable() {
        assert_eq!(
            format_compatibility_mismatch(
                "psycopg2_params unique violation",
                "SQLSTATE",
                "23505",
                "22P02"
            ),
            "psycopg2_params unique violation: expected SQLSTATE `23505`, got `22P02`"
        );
    }

    #[test]
    fn sqlstate_assert_accepts_expected_code() {
        assert_sqlstate(Some("23505"), "23505", "psycopg2_params unique violation");
    }

    #[test]
    fn sqlstate_assert_reports_missing_codes_clearly() {
        let panic = catch_unwind(|| {
            assert_sqlstate(None, "23505", "psycopg2_params unique violation");
        })
        .expect_err("assertion should panic");

        let message = panic_message(panic);
        assert!(message.contains("psycopg2_params unique violation"));
        assert!(message.contains("expected SQLSTATE `23505`, got `<missing>`"));
    }

    #[test]
    fn required_sqlstates_assert_ignores_order() {
        assert_required_sqlstates(
            ["23505", "22P02"],
            ["22P02", "23505"],
            "extractor_canary required sqlstates",
        );
    }

    #[test]
    fn required_sqlstates_assert_reports_missing_and_unexpected_codes() {
        let panic = catch_unwind(|| {
            assert_required_sqlstates(
                ["23505", "42P01"],
                ["23505", "22P02"],
                "extractor_canary required sqlstates",
            );
        })
        .expect_err("assertion should panic");

        let message = panic_message(panic);
        assert!(message.contains("extractor_canary required sqlstates"));
        assert!(message.contains("missing [22P02]"));
        assert!(message.contains("unexpected [42P01]"));
    }

    #[test]
    fn unsupported_refusal_defaults_to_feature_not_supported() {
        assert_unsupported_refusal_sqlstate(
            Some(DEFAULT_UNSUPPORTED_SQLSTATE),
            "psql_smoke unsupported SHOW ALL",
        );
    }

    fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
        match panic.downcast::<String>() {
            Ok(message) => *message,
            Err(panic) => match panic.downcast::<&'static str>() {
                Ok(message) => (*message).to_owned(),
                Err(_) => String::from("<non-string panic>"),
            },
        }
    }
}
