use std::collections::BTreeMap;

use serde_json::{Value, json};

pub(crate) type DifferentialRow = BTreeMap<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutcomeClass {
    Success,
    Refusal,
    Skip,
}

impl OutcomeClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Refusal => "refusal",
            Self::Skip => "skip",
        }
    }
}

#[must_use]
pub fn format_differential_mismatch(
    context: &str,
    expectation: &str,
    expected: &str,
    actual: &str,
) -> String {
    format!("{context}: expected {expectation} `{expected}`, got `{actual}`")
}

#[track_caller]
pub fn assert_command_tag(actual: &str, expected: &str, context: &str) {
    assert!(
        actual == expected,
        "{}",
        format_differential_mismatch(context, "command tag", expected, actual)
    );
}

#[track_caller]
pub fn assert_rows_affected(actual: u64, expected: u64, context: &str) {
    assert!(
        actual == expected,
        "{}",
        format_differential_mismatch(
            context,
            "rows affected",
            &expected.to_string(),
            &actual.to_string()
        )
    );
}

#[track_caller]
pub fn assert_sqlstate(actual: Option<&str>, expected: &str, context: &str) {
    let actual = actual.unwrap_or("<missing>");
    assert!(
        actual == expected,
        "{}",
        format_differential_mismatch(context, "SQLSTATE", expected, actual)
    );
}

#[track_caller]
pub fn assert_outcome_class(actual: OutcomeClass, expected: OutcomeClass, context: &str) {
    assert!(
        actual == expected,
        "{}",
        format_differential_mismatch(context, "outcome class", expected.as_str(), actual.as_str())
    );
}

#[track_caller]
pub fn assert_refusal_classification(actual: OutcomeClass, context: &str) {
    assert_outcome_class(actual, OutcomeClass::Refusal, context);
}

#[track_caller]
pub fn assert_skip_classification(actual: OutcomeClass, context: &str) {
    assert_outcome_class(actual, OutcomeClass::Skip, context);
}

#[track_caller]
pub fn assert_rowset_eq<I, J>(
    actual_columns: I,
    actual_rows: &[DifferentialRow],
    expected_columns: J,
    expected_rows: &[DifferentialRow],
    context: &str,
) where
    I: IntoIterator,
    I::Item: AsRef<str>,
    J: IntoIterator,
    J::Item: AsRef<str>,
{
    let actual_columns = collect_columns(actual_columns);
    let expected_columns = collect_columns(expected_columns);

    let columns_match = actual_columns == expected_columns;
    let rows_match = actual_rows == expected_rows;

    assert!(
        columns_match && rows_match,
        "{}",
        format!(
            "{context}: rowset mismatch; expected {}; actual {}",
            render_rowset(&expected_columns, expected_rows),
            render_rowset(&actual_columns, actual_rows)
        )
    );
}

fn collect_columns<I>(columns: I) -> Vec<String>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    columns
        .into_iter()
        .map(|column| column.as_ref().to_owned())
        .collect()
}

fn render_rowset(columns: &[String], rows: &[DifferentialRow]) -> String {
    serde_json::to_string(&json!({
        "columns": columns,
        "rows": rows,
    }))
    .expect("serialize differential rowset")
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    use serde_json::{Value, json};

    use super::{
        DifferentialRow, OutcomeClass, assert_command_tag, assert_outcome_class,
        assert_refusal_classification, assert_rows_affected, assert_rowset_eq,
        assert_skip_classification, assert_sqlstate, format_differential_mismatch,
    };

    #[test]
    fn differential_mismatch_format_is_stable() {
        assert_eq!(
            format_differential_mismatch(
                "write corpus insert tag",
                "command tag",
                "INSERT",
                "UPDATE"
            ),
            "write corpus insert tag: expected command tag `INSERT`, got `UPDATE`"
        );
    }

    #[test]
    fn command_tag_assert_accepts_exact_match() {
        assert_command_tag("INSERT", "INSERT", "write corpus insert tag");
    }

    #[test]
    fn rows_affected_assert_reports_clear_diff() {
        let panic = catch_unwind(|| {
            assert_rows_affected(2, 1, "write corpus upsert row count");
        })
        .expect_err("assertion should panic");

        let message = panic_message(panic);
        assert!(message.contains("write corpus upsert row count"));
        assert!(message.contains("expected rows affected `1`, got `2`"));
    }

    #[test]
    fn sqlstate_assert_reports_missing_code() {
        let panic = catch_unwind(|| {
            assert_sqlstate(None, "23505", "write corpus unique violation");
        })
        .expect_err("assertion should panic");

        let message = panic_message(panic);
        assert!(message.contains("write corpus unique violation"));
        assert!(message.contains("expected SQLSTATE `23505`, got `<missing>`"));
    }

    #[test]
    fn outcome_class_helpers_cover_refusal_and_skip() {
        assert_outcome_class(
            OutcomeClass::Success,
            OutcomeClass::Success,
            "read corpus success classification",
        );
        assert_refusal_classification(OutcomeClass::Refusal, "read corpus refusal classification");
        assert_skip_classification(OutcomeClass::Skip, "read corpus skip classification");
    }

    #[test]
    fn rowset_assert_accepts_matching_columns_and_rows() {
        let rows = vec![row([
            ("deal_id", json!("deal-1")),
            ("tenant_id", json!("tenant-a")),
        ])];

        assert_rowset_eq(
            ["deal_id", "tenant_id"],
            &rows,
            ["deal_id", "tenant_id"],
            &rows,
            "read corpus point lookup",
        );
    }

    #[test]
    fn rowset_assert_reports_stable_expected_and_actual_shapes() {
        let expected_rows = vec![row([
            ("deal_id", json!("deal-1")),
            ("tenant_id", json!("tenant-a")),
        ])];
        let actual_rows = vec![row([
            ("deal_id", json!("deal-2")),
            ("tenant_id", json!("tenant-a")),
        ])];

        let panic = catch_unwind(|| {
            assert_rowset_eq(
                ["deal_id", "tenant_id"],
                &actual_rows,
                ["deal_id", "tenant_id"],
                &expected_rows,
                "read corpus point lookup",
            );
        })
        .expect_err("assertion should panic");

        let message = panic_message(panic);
        assert!(message.contains("read corpus point lookup: rowset mismatch"));
        assert!(message.contains("\"columns\":[\"deal_id\",\"tenant_id\"]"));
        assert!(message.contains("\"deal_id\":\"deal-1\""));
        assert!(message.contains("\"deal_id\":\"deal-2\""));
    }

    fn row<const N: usize>(entries: [(&str, Value); N]) -> DifferentialRow {
        entries
            .into_iter()
            .map(|(column, value)| (column.to_owned(), value))
            .collect()
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
