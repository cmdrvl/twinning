//! The tests of verify's `crates/verify-cli/src/redact.rs` (v0.7.5), kept against twinning's
//! copy of those rules (`twinning-kernel/src/redaction.rs`) so that the copy cannot change
//! unnoticed. They live in the root package because that is what CI's `cargo test` runs.
//!
//! verify's test of unprinted rows has no counterpart: twinning prints every affected entry.

use std::{collections::BTreeMap, ffi::OsString};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use twinning::redaction::{
    HMAC_PREFIX, KeyError, REF_PREFIX, RedactionKey, VerifyRedaction, redact_report,
};
use verify_core::{
    constraint::Severity,
    refusal::RefusalCode,
    report::{AffectedEntry, ExecutionMode, ResultStatus, RuleResult, VerifyReport},
};

fn entry(key: &str, value: &str) -> AffectedEntry {
    AffectedEntry {
        binding: "loans".to_owned(),
        key: Some(BTreeMap::from([("loan_id".to_owned(), json!(key))])),
        field: Some("balance".to_owned()),
        value: Some(json!(value)),
    }
}

fn rule(id: &str, entries: Vec<AffectedEntry>) -> RuleResult {
    RuleResult {
        rule_id: id.to_owned(),
        severity: Severity::Error,
        status: ResultStatus::Fail,
        violation_count: entries.len(),
        affected: entries,
    }
}

fn report_with(results: Vec<RuleResult>) -> VerifyReport {
    // A neutral constraint hash, so "no sha256 text anywhere" checks only the redacted values.
    let mut report = VerifyReport::new(ExecutionMode::Embedded, "set", "constraint-hash");
    report.results = results;
    report
}

fn refs(report: &VerifyReport) -> Vec<(String, String)> {
    let text = |value: &Value| value.as_str().expect("a marker").to_owned();
    report
        .results
        .iter()
        .flat_map(|result| &result.affected)
        .map(|entry| {
            (
                text(&entry.key.as_ref().expect("key")["loan_id"]),
                text(entry.value.as_ref().expect("value")),
            )
        })
        .collect()
}

fn pair(key: &str, value: &str) -> (String, String) {
    (key.to_owned(), value.to_owned())
}

fn key(text: &str) -> RedactionKey {
    RedactionKey::from_env_value(Some(text.into()))
        .expect("usable key")
        .expect("a key")
}

#[test]
fn ordinals_follow_first_appearance_and_equal_values_share_a_ref() {
    let mut report = report_with(vec![rule(
        "R",
        vec![entry("A", "x"), entry("B", "x"), entry("A", "y")],
    )]);
    redact_report(&mut report, None);
    assert_eq!(
        refs(&report),
        vec![
            pair("redacted:ref:k1", "redacted:ref:v1"),
            pair("redacted:ref:k2", "redacted:ref:v1"),
            pair("redacted:ref:k1", "redacted:ref:v2"),
        ]
    );
    let text = serde_json::to_string(&report).expect("serializes");
    assert!(!text.contains("\"A\"") && !text.contains("\"x\""), "{text}");
}

#[test]
fn keys_and_cells_with_equal_bytes_use_separate_namespaces() {
    let mut report = report_with(vec![rule("R", vec![entry("same", "same")])]);
    redact_report(&mut report, None);
    assert_eq!(
        refs(&report),
        vec![pair("redacted:ref:k1", "redacted:ref:v1")]
    );
}

#[test]
fn ordinals_continue_across_rules() {
    let mut report = report_with(vec![
        rule("R1", vec![entry("a", "1")]),
        rule("R2", vec![entry("z", "9"), entry("a", "1")]),
    ]);
    redact_report(&mut report, None);
    assert_eq!(
        refs(&report),
        vec![
            pair("redacted:ref:k1", "redacted:ref:v1"),
            pair("redacted:ref:k2", "redacted:ref:v2"),
            pair("redacted:ref:k1", "redacted:ref:v1"),
        ]
    );
}

#[test]
fn default_output_contains_no_value_derived_hash() {
    let mut report = report_with(vec![rule("R", vec![entry("CANARY-KEY", "CANARY-CELL")])]);
    redact_report(&mut report, None);
    let text = serde_json::to_string(&report).expect("serializes");
    for canary in ["CANARY-KEY", "CANARY-CELL"] {
        let canonical = serde_json::to_string(&json!(canary)).expect("serializes");
        let old = format!("{:x}", Sha256::digest(canonical.as_bytes()));
        assert!(!text.contains(&old[..16]) && !text.contains(&old), "{text}");
    }
    assert!(!text.contains("sha256"), "{text}");
    assert!(!text.contains("hmac"), "{text}");
}

#[test]
fn key_validation_refuses_empty_and_short_and_accepts_16() {
    assert!(
        RedactionKey::from_env_value(None)
            .expect("unset is fine")
            .is_none()
    );
    assert_eq!(
        RedactionKey::from_env_value(Some("".into())),
        Err(KeyError::Empty)
    );
    assert_eq!(
        RedactionKey::from_env_value(Some("0123456789abcde".into())),
        Err(KeyError::TooShort)
    );
    assert!(
        RedactionKey::from_env_value(Some("0123456789abcdef".into()))
            .expect("16 bytes")
            .is_some()
    );
}

#[test]
fn keyed_markers_match_the_independent_known_answer_and_hide_the_key() {
    // The same known answer as verify's test, cross-checked there with:
    // printf '%s' '"CANARY-CELL-77c2d4"' | openssl dgst -sha256 -hmac 0123456789abcdef
    //   f90d3ee3380b0597dd4632a72c95605640d12893452bc855d916768ecd90b575
    let first = key("0123456789abcdef");
    assert_eq!(
        first.marker(&json!("CANARY-CELL-77c2d4")),
        "redacted:hmac-sha256:f90d3ee3380b0597dd4632a72c956056"
    );
    let other = key("0123456789abcdeg");
    assert_ne!(first.marker(&json!("v")), other.marker(&json!("v")));
    assert_eq!(first.marker(&json!("v")), first.marker(&json!("v")));
    assert!(!format!("{first:?}").contains("0123456789abcdef"));
    assert!(!format!("{:?}", VerifyRedaction::Keyed(first)).contains("0123456789abcdef"));
}

#[test]
fn keyed_mode_replaces_ordinals_and_is_stable_across_reports() {
    let key = key("0123456789abcdef");
    let mut first = report_with(vec![rule("R", vec![entry("A", "x")])]);
    let mut second = report_with(vec![rule("R", vec![entry("B", "q"), entry("A", "x")])]);
    redact_report(&mut first, Some(&key));
    redact_report(&mut second, Some(&key));
    assert_eq!(first.results[0].affected[0], second.results[0].affected[1]);
    let text = serde_json::to_string(&first).expect("serializes");
    assert!(
        text.contains(HMAC_PREFIX) && !text.contains(REF_PREFIX),
        "{text}"
    );
}

#[test]
fn refusals_that_can_quote_data_are_scrubbed() {
    let mut report = VerifyReport::refusal(
        ExecutionMode::Embedded,
        "set",
        "sha256:x",
        RefusalCode::SqlError,
        "SQL error: Could not convert string 'CANARY-CELL' to INT",
        json!({"rule_id": "R1", "message": "Could not convert string 'CANARY-CELL'",
               "key": {"id": "CANARY-KEY"}}),
    );
    redact_report(&mut report, None);
    let text = serde_json::to_string(&report).expect("serializes");
    assert!(!text.contains("CANARY"), "{text}");
    assert!(
        text.contains("R1") && text.contains("redacted:ref:k1"),
        "{text}"
    );
}

#[test]
fn explicit_wins_over_a_key_but_a_bad_key_is_still_refused() {
    let good = || Some(OsString::from("0123456789abcdef"));
    assert_eq!(
        VerifyRedaction::resolve(false, None),
        Ok(VerifyRedaction::Ordinal)
    );
    assert_eq!(
        VerifyRedaction::resolve(false, good()),
        Ok(VerifyRedaction::Keyed(key("0123456789abcdef")))
    );
    assert_eq!(
        VerifyRedaction::resolve(true, good()),
        Ok(VerifyRedaction::Explicit)
    );
    for explicit in [false, true] {
        assert_eq!(
            VerifyRedaction::resolve(explicit, Some("short".into())),
            Err(KeyError::TooShort)
        );
    }
    assert_eq!(VerifyRedaction::default(), VerifyRedaction::Ordinal);
}
